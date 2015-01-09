﻿module internal MBrace.SampleRuntime.Main

    open Nessos.Thespian
    open Nessos.Thespian.Remote.Protocols
    open MBrace
    open MBrace.Continuation
    open MBrace.Runtime
    open MBrace.SampleRuntime.Actors
    open MBrace.SampleRuntime.RuntimeProvider
    open MBrace.SampleRuntime.Tasks

    let maxConcurrentTasks = 10

    [<EntryPoint>]
    let main (args : string []) =
        try
            MBrace.SampleRuntime.Config.initRuntimeState()
            let address = MBrace.SampleRuntime.Config.getAddress()
            printfn "MBrace worker initialized on %O." address
            if args.Length > 0 then
                let runtime = Argument.toRuntime args
                let workerRef = new Worker(System.Diagnostics.Process.GetCurrentProcess().Id.ToString()) :> IWorkerRef
                let localRuntime = LocalRuntimeState.InitLocal(workerRef, runtime)
                Async.RunSync (Worker.initWorker localRuntime maxConcurrentTasks)
                0
            else
                Actor.Stateful (new System.Threading.CancellationTokenSource()) Worker.workerManager
                |> Actor.rename "workerManager"
                |> Actor.publish [ Protocols.utcp() ]
                |> Actor.start
                |> ignore
                Async.RunSynchronously <| async { while true do do! Async.Sleep 10000 }
                0
        with e ->
            printfn "Unhandled exception : %O" e
            let _ = System.Console.ReadKey()
            1
