(*
FSCdrQueue - reliable CDR queue processing for FreeSWITCH
Copyright (C) 2011 Michael Giagnocavo <mgg@giagnocavo.net>

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
*)
module FSCdrQueue

open System
open System.Net
open System
open System.Data.SqlClient

(*
The queue must distinguish betweeen a-legs and b-legs for backend queueing.
No processing of the XML is needed, just the leg type, guid, and compressed (QuickLZ) data.
A-legs are detected by the "a_" prefix in the log dir, or by "leg=a" in the querystring. ("leg=b" for b-legs).
*)

type SqlCommand with member x.AsyncExecuteNonQuery() = Async.FromBeginEnd(x.BeginExecuteNonQuery, x.EndExecuteNonQuery)

let log = log4net.LogManager.GetLogger("FSCdrQueue")

let compress = QuickLZSharp.QuickLZ.compress

let configDefault (name : string) value = 
    match Configuration.ConfigurationSettings.get_AppSettings().[name] with
    | null | "" -> value
    | x -> x

let httpPrefix = configDefault "httpPrefix" "http://localhost/fscdrqueue/"
/// Path CDRs logged by FreeSWITCH
let loggedCdrsPath = IO.Path.Combine(Environment.CurrentDirectory, configDefault "loggedCdrsPath" @"C:\freeswitch\log\xml-cdr\")
let connectionString = configDefault "connectionString" "Server=(local);Database=CdrDatabase;Integrated Security=true;Asynchronous Processing=true"
/// Minimum threads to keep available at all times
let minThreads = int <| configDefault "minThreads" "16"
/// Maximum threads and HTTP connections per server
let maxThreads = int <| configDefault "maxThreads" "128"

let sqlRetryTime = TimeSpan.FromSeconds(float <| configDefault "sqlRetryTimeSeconds" "15")

/// Simple module to have a timed multithreaded error status and event
module SqlRetry =
    let private sqlLock = obj()
    let private inError = ref false
    /// Set this to handle recovery code after error is cleared
    let mutable onErrorCleared : unit -> unit = ignore
    let isInError() = lock sqlLock (fun () -> !inError)
    let setError() = lock sqlLock (fun () -> 
        if not !inError then
            inError := true        
            Async.Start (async {
                do! Async.Sleep (int sqlRetryTime.TotalMilliseconds)
                log.Info(sprintf "SqlRetry timeout expired (%d seconds), clearing error status." (int sqlRetryTime.TotalSeconds))
                inError := false 
                onErrorCleared() }))

let mutable isRunning = false
let getIsRunning() = Threading.Thread.MemoryBarrier(); isRunning
let shutdownEvent = new Threading.ManualResetEvent(false)

let sendCdr isALeg (guid: Guid) (bytes : byte array) = async {
    if SqlRetry.isInError() || not (getIsRunning()) then return false 
    else
    try
        use conn = new SqlConnection(connectionString)
        use comm = new SqlCommand((if isALeg then "dbo.QueueALeg" else "dbo.QueueBLeg"), conn)
        comm.CommandType <- Data.CommandType.StoredProcedure
        if not isALeg then comm.Parameters.Add("blegGuid", Data.SqlDbType.UniqueIdentifier).Value <- guid
        comm.Parameters.Add("@data", Data.SqlDbType.VarBinary).Value <- bytes
        conn.Open()
        let! _ = comm.AsyncExecuteNonQuery()
        return true
    with :? SqlException | :? TimeoutException as ex ->
        log.Error("Error sending CDR, switching to disk.", ex)
        SqlRetry.setError()
        return false }

let saveCdr isALeg uuid bytes = 
    let path = IO.Path.Combine(loggedCdrsPath, sprintf "%s%s.cdr.xml.qlz" (if isALeg then "a_" else "") uuid)
    use fs = IO.File.Open(path, IO.FileMode.CreateNew, IO.FileAccess.Write, IO.FileShare.None)
    fs.Write(bytes, 0, bytes.Length)
    fs.Flush()

/// Runs continuously, moving FS logged files (.cdr.xml) to compressed files, ready for transmission (.cdr.xml.qlz)
let moveLoggedCdrsLoop () =
    while getIsRunning() do
        for srcpath in IO.Directory.GetFiles(loggedCdrsPath, "*.cdr.xml") do
            if not (getIsRunning()) then () else // Stop early
            let destpath = IO.Path.Combine(loggedCdrsPath, sprintf "%s.qlz" <| IO.Path.GetFileName srcpath)
            try // Keep source file open and locked until dest file is safely written, then delete
                let bytes = compress <| IO.File.ReadAllBytes srcpath
                IO.File.WriteAllBytes(destpath, bytes)
                IO.File.Delete srcpath
            with ex ->
                log.Fatal(sprintf "Error moving logged file %s." srcpath, ex)
                // On error, delete the dest
                if IO.File.Exists destpath then IO.File.Delete destpath
        shutdownEvent.WaitOne(sqlRetryTime) |> ignore

let runCdrFile filename = async {
    try let bytes = IO.File.ReadAllBytes filename
        let name = IO.Path.GetFileName(filename)
        let name = name.Substring(0, name.IndexOf('.'))
        let isALeg = name.StartsWith("a_")
        let guid = Guid(name.Replace("a_", ""))
        let! sent = sendCdr isALeg guid bytes
        if sent then IO.File.Delete filename
    with ex ->
        log.Fatal(sprintf "Fatal error sending file %s." filename, ex) }

let runQueuedFiles () = 
    IO.Directory.GetFiles(loggedCdrsPath, "*.cdr.xml.qlz")
        |> Seq.iter (runCdrFile >> Async.RunSynchronously)

SqlRetry.onErrorCleared <- runQueuedFiles

let processHttpContext (ctx : HttpListenerContext) = async {
    try
        let uuid = ctx.Request.QueryString.["uuid"]
        let isALeg = ctx.Request.QueryString.["leg"] = "a"
        use br = new IO.BinaryReader(ctx.Request.InputStream)
        let bytes = compress <| br.ReadBytes(int ctx.Request.ContentLength64)
        let! sent = sendCdr isALeg (Guid(uuid)) bytes
        if not sent then saveCdr isALeg uuid bytes 
        ctx.Response.StatusCode <- 200
        ctx.Response.Close()
    with ex ->
        log.Error("Could not receive HTTP message.", ex)
        let resp = Text.Encoding.UTF8.GetBytes(ex.ToString())
        ctx.Response.StatusCode <- 500
        ctx.Response.Close(resp, false) }

let listener = new HttpListener()
let httpListen () =
    let die (ex: exn) =
        log.Fatal("Error processing HTTP request, waiting 5 seconds before shutdown.", ex)
        Threading.Thread.Sleep(5000)
        Environment.Exit 1        
    try 
        isRunning <- true    
        listener.Prefixes.Add(httpPrefix)
        listener.Start()
        while getIsRunning() do
            try 
                let ctx = listener.GetContext()
                Async.Start (processHttpContext ctx)
            with ex ->
                if not (getIsRunning()) then () else die ex // If it's not running, then it's shutting down so exception is ok
        listener.Close()
        Environment.Exit 0
    with ex -> 
        die ex

let httpThread = Threading.Thread(httpListen)
let moveLoggedThread = Threading.Thread(moveLoggedCdrsLoop)

let shutdown () = 
    isRunning <- false
    listener.Stop()    
    shutdownEvent.Set() |> ignore
    httpThread.Join()
    moveLoggedThread.Join()

type FSCdrQueueService() = 
    inherit ServiceProcess.ServiceBase()
    do base.ServiceName <- "FSCdrQueue"
    override x.OnStart(args) = 
        isRunning <- true
        runQueuedFiles() 
        httpThread.Start()
        moveLoggedThread.Start()
    override x.OnStop() = 
        shutdown()

let main() =
    Threading.ThreadPool.SetMinThreads(minThreads, minThreads * 2) |> ignore
    let mt = max minThreads maxThreads
    Threading.ThreadPool.SetMaxThreads(mt, mt * 8) |> ignore
    ServicePointManager.DefaultConnectionLimit <- mt * 8
    ServicePointManager.Expect100Continue <- false

    if Environment.UserInteractive then
        isRunning <- true
        runQueuedFiles() 
        httpThread.Start()
        moveLoggedThread.Start()
        printfn "Started, press enter to exit."
        Console.ReadLine() |> ignore
        shutdown()
    else
        ServiceProcess.ServiceBase.Run(new FSCdrQueueService())

[<assembly: log4net.Config.XmlConfigurator(Watch = true)>]
do main()
