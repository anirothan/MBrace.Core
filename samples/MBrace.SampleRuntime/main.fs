﻿module internal Nessos.MBrace.SampleRuntime.Main

    open Nessos.Thespian
    open Nessos.Thespian.Remote.Protocols
    open Nessos.MBrace
    open Nessos.MBrace.Continuation
    open Nessos.MBrace.Runtime
    open Nessos.MBrace.SampleRuntime.Actors
    open Nessos.MBrace.SampleRuntime.RuntimeProvider
    open Nessos.MBrace.SampleRuntime.Tasks

    let maxConcurrentTasks = 10

    [<EntryPoint>]
    let main (args : string []) =
        try
            Nessos.MBrace.SampleRuntime.Config.initRuntimeState()
            let address = Nessos.MBrace.SampleRuntime.Config.getAddress()
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
