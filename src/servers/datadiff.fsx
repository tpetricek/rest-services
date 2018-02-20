#if INTERACTIVE
#I "../../packages"
#r "../packages/DynamicInterop/lib/netstandard1.2/DynamicInterop.dll"
#r "../packages/R.NET/lib/net40/RDotNet.dll"
#r "../packages/R.NET.FSharp/lib/net40/RDotNet.FSharp.dll"
#r "Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#r "Suave/lib/net40/Suave.dll"
#r "FSharp.Data/lib/net45/FSharp.Data.dll"
#r "System.Transactions"
#I "../common"
#load "serializer.fs"
#load "pivot.fs"
#else
module TheGamma.Services.Datadiff
#endif
open System
open System.IO
open System.Threading.Tasks
open System.Collections.Generic
open FSharp.Data
open Newtonsoft.Json
open TheGamma.Services
open TheGamma.Services.Serializer
open TheGamma.Services.Pivot
open Suave.Logging
open Suave.Writers
open RDotNet

let logger = Targets.create Verbose [||]
let logf fmt = Printf.kprintf (fun s -> logger.info(Message.eventX s)) fmt

// ------------------------------------------------------------------------------------------------
// Thread-safe R engine access
// ------------------------------------------------------------------------------------------------

let queue = new System.Collections.Concurrent.BlockingCollection<_>()
let worker = System.Threading.Thread(fun () -> 
  
  logf "Starting R instance"
  let rpath = __SOURCE_DIRECTORY__ + "/../../rinstall/R-3.4.1" |> IO.Path.GetFullPath
  let pkgpath = __SOURCE_DIRECTORY__ + "/../../rinstall/libraries" |> IO.Path.GetFullPath
  //let rpath = @"C:\Programs\Academic\R\R-3.4.2"
  let path = System.Environment.GetEnvironmentVariable("PATH")
  System.Environment.SetEnvironmentVariable("PATH", sprintf "%s;%s/bin/x64" path rpath)
  System.Environment.SetEnvironmentVariable("R_HOME", rpath)
  let rengine = REngine.GetInstance(rpath + "/bin/x64/R.dll", AutoPrint=false)
  rengine.Evaluate(sprintf ".libPaths( c(\"%s\") )" (pkgpath.Replace("\\","\\\\"))) |> ignore
  //rengine.Evaluate(".libPaths( c( .libPaths(), \"C:\\\\Users\\\\tomas\\\\Documents\\\\R\\\\win-library\\\\3.4\") )") |> ignore
  rengine.Evaluate("print(.libPaths())") |> ignore

  logf "Loading datadiff"
  try
    rengine.Evaluate("library(datadiff)") |> ignore
  with e ->
    printfn "Failed loading datadiff: %A" e
  //rengine.Evaluate("""broadband2014sm <- broadband2014[,c("Urban.rural","Download.speed..Mbit.s..24.hrs","Upload.speed..Mbit.s.24.hour","DNS.resolution..ms.24.hour","Latency..ms.24.hour","Web.page..ms.24.hour")]""") |> ignore
  //rengine.Evaluate("ddf <- ddiff(broadband2015,broadband2014sm)") |> ignore
  //logf "Broadband diff completed"

  while true do 
    let op = queue.Take() 
    op rengine )
worker.Start()

let withREngine op = 
  Async.FromContinuations(fun (cont, econt, _) ->
    queue.Add(fun reng -> 
       let res = try Choice1Of2(op reng) with e -> Choice2Of2(e)
       match res with Choice1Of2 res -> cont res | Choice2Of2 e -> econt e    
    )
  )


let getConvertor (rengine:REngine) typ =
  let stringConvertor data =
    data |> Array.map (function 
      | JsonValue.String s -> s 
      | v -> v.ToString() ) |> rengine.CreateCharacterVector :> SymbolicExpression
  let withFallback f data : SymbolicExpression =
    try f data with _ -> stringConvertor data 
  match typ with 
  | InferredType.OneZero _ 
  | InferredType.Bool _ -> withFallback (fun data -> 
      data |> Array.map (function 
        | JsonValue.Boolean b -> b 
        | JsonValue.Number 1.0M -> true
        | JsonValue.Number 0.0M -> false
        | v -> failwithf "Expected boolean, but got: %A" v) |> rengine.CreateLogicalVector :> _)
  | InferredType.Int _ 
  | InferredType.Float _ -> withFallback (fun data -> 
      let line = data |> Array.map (function 
        | JsonValue.Number n -> string (float n)
        | JsonValue.Float n -> string n 
        | JsonValue.String "" -> "NA"
        | JsonValue.String s -> string (float s)
        | v -> failwithf "Expected number, but got: %A" v) |> String.concat "," |> sprintf "c(%s)" 
      rengine.Evaluate(line).AsNumeric() :> _)
  | InferredType.Date _
  | InferredType.Any
  | InferredType.Empty
  | InferredType.String _ -> stringConvertor 

let createRDataFrame (rengine:REngine) data = 
  let tmpEnv = rengine.Evaluate("new.env()").AsEnvironment()
  rengine.SetSymbol("tmpEnv", tmpEnv)

  let propNames = (Seq.head data:JsonValue).Properties() |> Seq.map fst
  let propConvs = 
    data
    |> Seq.truncate 500
    |> Seq.map (fun (r:JsonValue) -> 
        r.Properties() |> Array.map (function
          | _, JsonValue.Boolean b -> b.ToString()
          | _, JsonValue.Number n -> n.ToString()
          | _, JsonValue.Float f -> f.ToString()
          | _, JsonValue.String s -> s.ToString()
          | _ -> failwith "createRDataFrame: Unexpected value" ) )
    |> Seq.map (Array.map Inference.inferType)
    |> Seq.reduce (Array.map2 Inference.unifyTypes)
    |> Seq.map (getConvertor rengine)

  let props = Seq.zip propNames propConvs
  let cols = props |> Seq.map (fun (p, _) -> p, ResizeArray<_>()) |> dict
  for row in data do
    match row with 
    | JsonValue.Record recd -> for k, v in recd do cols.[k].Add(v)
    | _ -> failwith "Expected record"
  props |> Seq.iteri (fun i (n, conv) ->
    rengine.SetSymbol("tmp" + string i, conv (cols.[n].ToArray()), tmpEnv))  
  let assigns = props |> Seq.mapi (fun i (n, _) -> sprintf "'%s'=tmpEnv$tmp%d" n i)
  rengine.Evaluate(sprintf "tmpEnv$df <- data.frame(%s)" (String.concat "," assigns)) |> ignore
  rengine.Evaluate(sprintf "colnames(tmpEnv$df) <- c(%s)" (String.concat "," [ for n, _ in props -> "\"" + n + "\"" ])) |> ignore
  "tmpEnv$df"

// ------------------------------------------------------------------------------------------------
// Config and data loading helpers
// ------------------------------------------------------------------------------------------------

#if INTERACTIVE
let root = "http://localhost:10037"
#else
let root = "https://thegamma-rest-services.azurewebsites.net"
#endif

// ------------------------------------------------------------------------------------------------
//
// ------------------------------------------------------------------------------------------------

open Suave
open Suave.Filters
open Suave.Operators

type Patch = 
  | Permute of int list
  | Recode of int * (string*string) list
  | Insert of int * string (* * dataframe *)
  | Shift of int * float
  | Scale of int * float
  | Delete of int
  | Break // ????

let parsePatch (rengine:REngine) =
  let call (f, args) = 
    for i, e in Seq.indexed args do rengine.SetSymbol(sprintf "tmp%d" i, e)
    rengine.Evaluate(sprintf "%s(%s)" f (String.concat "," [ for i, _ in Seq.indexed args -> sprintf "tmp%d" i]))
  
  rengine.Evaluate("dd <- decompose_patch(ddf)") |> ignore

  [ for i in 1 .. rengine.Evaluate("dd").AsList().Length ->
      let typ = rengine.Evaluate(sprintf "patch_type(dd[[%d]])" i).AsCharacter() |> Seq.head
      let parnames = rengine.Evaluate(sprintf "names(get_patch_params(dd[[%d]]))" i).AsCharacter() |> List.ofSeq
      let getpar name = rengine.Evaluate(sprintf "get_patch_params(dd[[%d]])$%s" i name)
      match typ with
      | "break" -> Break // ???
      | "permute" -> 
          Permute(List.ofSeq(getpar("perm").AsInteger()))
      | "delete" -> 
          let col = getpar("cols").AsInteger() |> Seq.head
          Delete(col)
      | "recode" -> 
          let col = getpar("cols").AsInteger() |> Seq.head
          let origc = call("names", [getpar("encoding")]).AsCharacter() |> List.ofSeq
          let newc = getpar("encoding").AsCharacter() |> List.ofSeq
          Recode(col, List.zip origc newc)
      | "insert" -> 
          let ipt = getpar("insertion_point").AsInteger() |> Seq.head
          let insname = call("names", [getpar("data")]).AsCharacter() |> Seq.head
          Insert(ipt, insname)
      | "shift" -> 
          let col = getpar("cols").AsInteger() |> Seq.head
          let by = getpar("shift").AsNumeric() |> Seq.head
          Shift(col, by)
      | "scale" -> 
          let col = getpar("cols").AsInteger() |> Seq.head
          let by = getpar("scale_factor").AsNumeric() |> Seq.head
          Scale(col, by)
      | t -> failwithf "Unexpected patch (%d) type %s" i t ]
(*
let sample = "http://localhost:7102/434925e5/it"
let data = "http://localhost:7102/699b2df0/it"
*)
let dataDiffAgent = MailboxProcessor.Start(fun inbox -> async {
  let cache = System.Collections.Generic.Dictionary<_, _>() 
  while true do 
    try
      let! sample, data, (repl:AsyncReplyChannel<_>) = inbox.Receive() 
      logf "datadiff(sample=%s, data=%s)" sample data
      let key = sample + data
      if not (cache.ContainsKey(key)) then
        logf "downloading data and running ddiff"
        let! sampleStr = Http.AsyncRequestString(sample)
        let! dataStr = Http.AsyncRequestString(data)
        let sample = JsonValue.Parse(sampleStr)
        let data = JsonValue.Parse(dataStr)
        let getCols (js:JsonValue) = js.AsArray().[0].Properties() |> Array.map fst
        let! patches = withREngine (fun rengine ->
          rengine.Evaluate(sprintf "smpl <- %s" (createRDataFrame rengine (sample.AsArray()))) |> ignore
          rengine.Evaluate(sprintf "data <- %s" (createRDataFrame rengine (data.AsArray()))) |> ignore
          rengine.Evaluate("ddf <- ddiff(data, smpl, patch_generators=list(gen_patch_affine,gen_patch_insert))") |> ignore
          parsePatch rengine )
        cache.Add(key, (getCols sample, getCols data, patches, data))
      else logf "using cached value"
      repl.Reply(cache.[key])
    with e ->
      printfn "Agent failed: %A" e })  

let dataDiff sample data = dataDiffAgent.PostAndAsyncReply(fun repl -> sample, data, repl)

let xcookie f ctx = async {
  match ctx.request.headers |> Seq.tryFind (fun (h, _) -> h.ToLower() = "x-cookie") with
  | Some(_, v) -> 
      let cks = v.Split([|"&"|], StringSplitOptions.RemoveEmptyEntries) |> Array.map (fun k -> 
        match k.Split('=') with [|k;v|] -> k, System.Web.HttpUtility.UrlDecode v | _ -> failwith "Wrong cookie!") |> dict
      return! f cks ctx
  | _ -> return None }

let app = 
  choose [
    path "/" >=> returnMembers [
      let pars = 
        [ Parameter("data", Type.Named("frame"), false, ParameterKind.Static("data"))
          Parameter("sample", Type.Named("frame"), false, ParameterKind.Static("sample")) ]
      yield Member("adapt", Some pars, Result.Nested("/ddiff"), [], [])
    ]
    
    path "/ddiff" >=> xcookie (fun ck ->
      let sample = ck.["sample"].Replace('/','_') |> System.Web.HttpUtility.UrlEncode
      let data = ck.["data"].Replace('/','_') |> System.Web.HttpUtility.UrlEncode
      returnMembers [
        Member("then", None, Nested(sprintf "/adapt/%s/%s/then" sample data), [], [])
      ]
    )

    pathScan "/data/%s/%s/then%s" (fun (sample, data, url) ctx -> async {
      
      // Get applied patches
      let filter = url.Split([|'/'|], System.StringSplitOptions.RemoveEmptyEntries) |> Array.map int |> set
      let! sampleCols, cols, allPatches, frame = dataDiff (sample.Replace('_','/')) (data.Replace('_','/'))
      let colName i = cols.[i-1]
      let patches = allPatches |> List.indexed |> List.filter (function 
        | i, Delete _ when filter.Contains(-1) -> true
        | i, _ when filter.Contains i -> true
        | _ -> false) |> List.map snd

      // If we want to apply permute, it will pick columns with these names
      let permuteColNames = allPatches |> List.fold (fun cols patch ->
        match patch with 
        | Delete i -> Array.filter ((<>) (colName i)) cols
        | Permute perm -> [| for i in perm -> cols.[i-1] |]
        | Break _ -> cols
        | Shift _ | Scale _ | Recode _ -> cols
        | _ -> failwith "TODO: Unsupported patch" ) cols

      let rows = frame.AsArray() |> Array.map (fun rcd -> rcd.Properties())

      // Apply all selected Deletes
      let deleteCols = patches |> List.choose (function Delete col -> Some(colName col) | _ -> None) |> set
      let rows = rows |> Array.map (Array.filter (fst >> deleteCols.Contains >> not)) 
      
      // Apply permute 
      let hasPermute = patches |> List.exists (function Permute _ -> true | _ -> false)
      let rows = 
        if hasPermute then
          let permuteColNamesSet = set permuteColNames
          rows |> Array.map (fun row ->
            [| for i, pc in Seq.indexed permuteColNames do
                 let k, v = Array.find (fun (k,v) -> k = pc) row 
                 yield sampleCols.[i], v
               for k, v in row do
                  if not(permuteColNamesSet.Contains(k)) then yield k, v |])
        else rows

      let json = JsonValue.Array(Array.map JsonValue.Record rows).ToString()
      return! ctx |> Successful.OK json } )

    pathScan "/adapt/%s/%s/then%s" (fun (sample, data, url) ctx -> async {
      logf "adapt(sample=%s, data=%s, url=%s)" sample data url

      let! _, cols, patches, frame = dataDiff (sample.Replace('_','/')) (data.Replace('_','/'))
      let patches = Seq.indexed patches
      let filter = url.Split([|'/'|], System.StringSplitOptions.RemoveEmptyEntries) |> Array.map int |> set
      let filter = 
        if not (filter.Contains -1) then filter else
        Set.union filter (set [ for i, p in patches do match p with Delete _ -> yield i | _ -> () ])

      let sample, data = System.Web.HttpUtility.UrlEncode(sample), System.Web.HttpUtility.UrlEncode(data)
      let colName i = cols.[i-1]
      return! ctx |> returnMembers [
        let dataUrl = sprintf "/data/%s/%s/then/%s" sample data (String.concat "/" [ for i in filter -> string i])
        let sch = [ Schema("http://schema.thegamma.net", "CompletionItem", ["hidden", JsonValue.Boolean true ]) ]
        yield Member("preview", None, Primitive(Type.Seq(Type.Record ["Testing", Type.Named("float")]), dataUrl), [], sch)
        yield Member("Result", None, Primitive(Type.Seq(Type.Record ["Testing", Type.Named("float")]), dataUrl), [], [])
        
        let deleteAllUrl = sprintf "/adapt/%s/%s/then/-1/%s" sample data (String.concat "/" [ for i in filter -> string i])
        yield Member("Delete all recommended columns", None, Nested(deleteAllUrl), [], [])

        for i, patch in patches do
          if filter.Contains(i) then () else
          let mkMember fmt = Printf.kprintf (fun s -> 
            let url = sprintf "/adapt/%s/%s/then/%s" sample data (String.concat "/" ((string i)::[ for i in filter -> string i]))
            Member(s, None, Nested(url), [], [])) fmt
          match patch with  
          | Permute _ -> yield mkMember "Permute columns"
          | Delete(col) -> yield mkMember "Delete column %s" (colName col)
          | Recode(col, coding) -> yield mkMember "Recode column %s" (colName col)
          | Insert(col, name) -> yield mkMember "Insert column %s at %d" name col
          | Shift(col, by) -> yield mkMember "Shift column %s by %f" (colName col) by
          | Scale(col, by) -> yield mkMember "Scale column %s by %f" (colName col) by
          | Break -> () // ???
      ]
    })
  ]
