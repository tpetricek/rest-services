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
  //let rpath = __SOURCE_DIRECTORY__ + "/../rinstall/R-3.4.1" |> IO.Path.GetFullPath
  let rpath = @"C:\Programs\Academic\R\R-3.4.2"
  let path = System.Environment.GetEnvironmentVariable("PATH")
  System.Environment.SetEnvironmentVariable("PATH", sprintf "%s;%s/bin/x64" path rpath)
  System.Environment.SetEnvironmentVariable("R_HOME", rpath)
  let rengine = REngine.GetInstance(rpath + "/bin/x64/R.dll", AutoPrint=false)
  rengine.Evaluate(".libPaths( c( .libPaths(), \"C:\\\\Users\\\\tomas\\\\Documents\\\\R\\\\win-library\\\\3.4\") )") |> ignore
  rengine.Evaluate("print(.libPaths())") |> ignore

  logf "Calculating data diff of broadband dataset"
  rengine.Evaluate("library(datadiff)") |> ignore
  rengine.Evaluate("ddf <- ddiff(broadband2014, broadband2015)") |> ignore
  logf "Broadband diff completed"

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

let parsePatch = withREngine (fun rengine ->
  let call (f, args) = 
    for i, e in Seq.indexed args do rengine.SetSymbol(sprintf "tmp%d" i, e)
    rengine.Evaluate(sprintf "%s(%s)" f (String.concat "," [ for i, _ in Seq.indexed args -> sprintf "tmp%d" i]))
  
  rengine.Evaluate("dd <- decompose_patch(ddf)") |> ignore

  rengine.Evaluate("names(broadband2014)").AsCharacter() |> List.ofSeq,
  [ for i in 1 .. rengine.Evaluate("dd").AsList().Length ->
      let typ = rengine.Evaluate(sprintf "patch_type(dd[[%d]])" i).AsCharacter() |> Seq.head
      let parnames = rengine.Evaluate(sprintf "names(get_patch_params(dd[[%d]]))" i).AsCharacter() |> List.ofSeq
      let getpar name = rengine.Evaluate(sprintf "get_patch_params(dd[[%d]])$%s" i name)
      match typ with
      | "permute" -> 
          Permute(List.ofSeq(getpar("perm").AsInteger()))
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
      | t -> failwithf "Unexpected patch type %s" t ])

let app = 
  choose [
    (fun ctx -> async {
      let filter = ctx.request.url.LocalPath.Split([|'/'|], System.StringSplitOptions.RemoveEmptyEntries) |> Array.map int |> set
      let! cols, patches = parsePatch
      let colName i = cols.[i-1]
      let patches = Seq.indexed patches
      return! ctx |> returnMembers [
        for i, patch in patches do
          if filter.Contains(i) then () else
          let mkMember fmt = Printf.kprintf (fun s -> 
            let url = 
              if ctx.request.url.LocalPath = "/" then "/" + string i
              else ctx.request.url.LocalPath + "/" + string i
            Member(s, None, Nested(url), [], [])) fmt
          match patch with  
          | Permute _ -> yield mkMember "Permute columns"
          | Recode(col, coding) -> yield mkMember "Recode column %s" (colName col)
          | Insert(col, name) -> yield mkMember "Insert column %s at %d" name col
          | Shift(col, by) -> yield mkMember "Shift column %s by %f" (colName col) by
          | Scale(col, by) -> yield mkMember "Scale column %s by %f" (colName col) by
      ]
    })
  ]
