﻿//namespace MBrace
//
//
//open MBrace.Continuation
//
///// Represent a distributed atomically updatable value reference
//type ICloudAtom<'T> =
//    inherit ICloudDisposable
//
//    /// Cloud atom identifier
//    abstract Id : string
//
//    /// Asynchronously returns the current value of atom.
//    abstract Value : Cloud<'T>
//
//    /// <summary>
//    ///     Atomically updates table entry of given id using updating function.
//    /// </summary>
//    /// <param name="updater">Value updating function</param>
//    /// <param name="maxRetries">Maximum retries under optimistic semantics. Defaults to infinite.</param>
//    abstract Update : updater:('T -> 'T) * ?maxRetries:int -> Cloud<unit>
//
//    /// <summary>
//    ///      Forces a value on atom.
//    /// </summary>
//    /// <param name="value">value to be set.</param>
//    abstract Force : value:'T -> Cloud<unit>

namespace MBrace.Store
 
//open MBrace

/// Defines a factory for distributed atoms
type ICloudAtomProvider =

    /// Implementation name
    abstract Name : string

    /// Cloud atom identifier
    abstract Id : string

    /// Create a uniquely specified container name.
    abstract CreateUniqueContainerName : unit -> string

    /// <summary>
    ///     Checks if provided value is supported in atom instances.
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    abstract IsSupportedValue : value:'T -> bool

    /// <summary>
    ///     Creates a new atom instance with given initial value. Returning an identifier.
    /// </summary>
    /// <param name="container">Atom container.</param>
    /// <param name="initValue">Initial value to occupy the atom.</param>
    abstract Create<'T> : container:string * initValue:'T -> Async<string>

    /// <summary>
    ///     Atomically updates table entry of given id using updating function.
    /// </summary>
    /// <param name="id">Atom identifier.</param>
    /// <param name="updater">Value updating function.</param>
    /// <param name="maxRetries">Maximum retries under optimistic semantics. Defaults to infinite.</param>
    abstract Update : id:string * updater:('T -> 'T) * ?maxretries:int -> Async<unit>

    /// <summary>
    ///      Forces a value on atom instance.
    /// </summary>
    ///
    /// <param name="id">Atom identifier.</param>
    /// <param name="value">value to be set.</param>
    abstract Force : id:string * value:'T -> Async<unit>

    /// <summary>
    ///     Disposes all atoms in provided container.
    /// </summary>
    /// <param name="container">Atom container.</param>
    abstract DisposeContainer : container:string -> Async<unit>

/// Atom configuration passed to the continuation execution context
type AtomConfiguration =
    {
        /// Atom provider instance
        AtomProvider : ICloudAtomProvider
        /// Default container for instance in current execution context.
        DefaultContainer : string
    }

//namespace MBrace
//
//open MBrace.Continuation
//open MBrace.Store
//
//#nowarn "444"
//
//type CloudAtom =
//    
//    /// <summary>
//    ///     Creates a new cloud atom instance with given value.
//    /// </summary>
//    /// <param name="initial">Initial value.</param>
//    static member New<'T>(initial : 'T) : Cloud<ICloudAtom<'T>> = cloud {
//        let! config = Cloud.GetResource<AtomConfiguration> ()
//        return! Cloud.OfAsync <| config.AtomProvider.CreateAtom(config.DefaultContainer, initial)
//    }
//
//    /// <summary>
//    ///     Dereferences a cloud atom.
//    /// </summary>
//    /// <param name="atom">Atom instance.</param>
//    static member Read(atom : ICloudAtom<'T>) : Cloud<'T> = cloud {
//        return! atom.Value
//    }
//
//    /// <summary>
//    ///     Atomically updates the contained value.
//    /// </summary>
//    /// <param name="updater">value updating function.</param>
//    /// <param name="atom">Atom instance to be updated.</param>
//    static member Update (updater : 'T -> 'T) (atom : ICloudAtom<'T>) : Cloud<unit> = cloud {
//        return! atom.Update updater
//    }
//
//    /// <summary>
//    ///     Forces the contained value to provided argument.
//    /// </summary>
//    /// <param name="value">Value to be set.</param>
//    /// <param name="atom">Atom instance to be updated.</param>
//    static member Force (value : 'T) (atom : ICloudAtom<'T>) : Cloud<unit> = cloud {
//        return! Cloud.OfAsync <| atom.Force value
//    }
//
//    /// <summary>
//    ///     Transactionally updates the contained value.
//    /// </summary>
//    /// <param name="transaction">Transaction function.</param>
//    /// <param name="atom">Atom to be transacted.</param>
//    static member Transact (transaction : 'T -> 'R * 'T) (atom : ICloudAtom<'T>) : Cloud<'R> = cloud {
//        let result = ref Unchecked.defaultof<'R>
//        do! atom.Update(fun t -> let r,t' = transaction t in result := r ; t')
//        return result.Value
//    }
//
//    /// <summary>
//    ///     Deletes the provided atom instance from store.
//    /// </summary>
//    /// <param name="atom">Atom instance to be deleted.</param>
//    static member Delete (atom : ICloudAtom<'T>) = Cloud.Dispose atom
//
//
//    /// <summary>
//    ///     Checks if value is supported by current table store.
//    /// </summary>
//    /// <param name="value">Value to be checked.</param>
//    static member IsSupportedValue(value : 'T) = cloud {
//        let! config = Cloud.TryGetResource<AtomConfiguration> ()
//        return
//            match config with
//            | None -> false
//            | Some ap -> ap.AtomProvider.IsSupportedValue value
//    }