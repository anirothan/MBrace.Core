﻿namespace MBrace.Store

open System
open System.Collections
open System.Collections.Generic
open System.Runtime.Serialization
open System.Text
open System.IO

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals

#nowarn "444"

/// <summary>
///     Ordered, immutable collection of values persisted in a single cloud file.
/// </summary>
[<DataContract; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type CloudSequence<'T> =

    // https://visualfsharp.codeplex.com/workitem/199
    [<DataMember(Name = "Path")>]
    val mutable private path : string
    [<DataMember(Name = "ETag")>]
    val mutable private etag : ETag
    [<DataMember(Name = "Count")>]
    val mutable private count : int64 option
    [<DataMember(Name = "Deserializer")>]
    val mutable private deserializer : (Stream -> seq<'T>) option
    [<DataMember(Name = "IsCacheEnabled")>]
    val mutable private enableCache : bool

    internal new (path, etag, count, deserializer, ?enableCache : bool) =
        let enableCache = defaultArg enableCache false
        { path = path ; etag = etag ; count = count ; deserializer = deserializer ; enableCache = enableCache }

    member private c.GetSequenceFromStore () = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let! deserializer = local {
            match c.deserializer with
            | Some ds -> return ds
            | None ->
                let! serializer = Cloud.GetResource<ISerializer> ()
                return fun s -> serializer.SeqDeserialize<'T>(s, leaveOpen = false)
        }

        // wrap stream inside enumerator to enforce correct IEnumerable behaviour
        let fileStore = config.FileStore
        let path = c.path
        let mkEnumerator () =
            let streamOpt = fileStore.TryBeginRead(path, c.etag) |> Async.RunSync
            match streamOpt with
            | None -> raise <| new InvalidDataException(sprintf "CloudSequence: incorrect etag in file '%s'." c.path)
            | Some stream -> deserializer(stream).GetEnumerator()

        return Seq.fromEnumerator mkEnumerator
    }

    interface ICloudCacheable<'T []> with
        member c.UUID = sprintf "CloudSequence:%s:%s" c.path c.etag
        member c.GetSourceValue () = local {
            let! seq = c.GetSequenceFromStore()
            return Seq.toArray seq
        }

    /// Returns an enumerable that lazily fetches elements of the cloud sequence from store.
    member c.ToEnumerable () = local {
        if c.CacheByDefault then
            let! array = CloudCache.GetCachedValue c
            return array :> seq<'T>
        else
            let! cachedValue = CloudCache.TryGetCachedValue c
            match cachedValue with
            | None -> return! c.GetSequenceFromStore()
            | Some cv -> return cv :> seq<'T>
    }

    /// Fetches all elements of the cloud sequence and returns them as a local array.
    member c.ToArray () : Local<'T []> = local { return! CloudCache.GetCachedValue(c, cacheIfNotExists = c.CacheByDefault) }

    /// Caches all elements to local execution context. Returns true if succesful.
    member c.ForceCache () = local { return! CloudCache.PopulateCache c }

    /// Indicates if array is cached in local execution context
    member c.IsCachedLocally = local { return! CloudCache.IsCachedEntity c }

    /// Path to Cloud sequence in store
    member c.Path = c.path

    member c.ETag = c.etag

    /// Enables implicit, on-demand caching of values when first dereferenced.
    member c.CacheByDefault = c.enableCache

    /// immutable update to the cache behaviour
    member internal c.WithCacheBehaviour b = new CloudSequence<'T>(c.path, c.etag, c.count, c.deserializer, b)

    /// Cloud sequence element count
    member c.Count = local {
        match c.count with
        | Some l -> return l
        | None ->
            // this is a potentially costly operation
            let! seq = c.ToEnumerable()
            let l = int64 <| Seq.length seq
            c.count <- Some l
            return l
    }

    /// Underlying sequence size in bytes
    member c.Size = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        return! ofAsync <| config.FileStore.GetFileSize c.path
    }

    interface ICloudStorageEntity with
        member c.Type = sprintf "CloudSequence:%O" typeof<'T>
        member c.Id = c.path

    interface ICloudDisposable with
        member c.Dispose () = local {
            let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
            return! ofAsync <| config.FileStore.DeleteFile c.path
        }

    interface ICloudCollection<'T> with
        member c.IsKnownCount = Option.isSome c.count
        member c.IsKnownSize = true
        member c.Count = c.Count
        member c.Size = c.Size
        member c.ToEnumerable() = c.ToEnumerable()

    override c.ToString() = sprintf "CloudSequence[%O] at %s" typeof<'T> c.path
    member private c.StructuredFormatDisplay = c.ToString()  

/// Partitionable implementation of cloud file line reader
[<DataContract>]
type private TextLineSequence(path : string, etag : ETag, ?encoding : Encoding, ?enableCache : bool) =
    inherit CloudSequence<string>(path, etag, None, Some(fun stream -> TextReaders.ReadLines(stream, ?encoding = encoding)), ?enableCache = enableCache)

    interface IPartitionableCollection<string> with
        member cs.GetPartitions(weights : int []) = local {
            let! size = CloudFile.GetSize cs.Path

            let mkRangedSeqs (weights : int[]) =
                let getDeserializer s e stream = TextReaders.ReadLinesRanged(stream, max (s - 1L) 0L, e, ?encoding = encoding)
                let mkRangedSeq rangeOpt =
                    match rangeOpt with
                    | Some(s,e) -> new CloudSequence<string>(cs.Path, cs.ETag, None, Some(getDeserializer s e), ?enableCache = enableCache) :> ICloudCollection<string>
                    | None -> new SequenceCollection<string>([||]) :> _

                let partitions = Array.splitWeightedRange weights 0L size
                Array.map mkRangedSeq partitions

            if size < 512L * 1024L then
                // partition lines in-memory if file is particularly small.
                let! count = cs.Count
                if count < int64 weights.Length then
                    let! lines = cs.ToArray()
                    let liness = Array.splitWeighted weights lines
                    return liness |> Array.map (fun lines -> new SequenceCollection<string>(lines) :> _)
                else
                    return mkRangedSeqs weights
            else
                return mkRangedSeqs weights
        }

type CloudSequence =

    /// <summary>
    ///     Creates a new Cloud sequence by persisting provided sequence as a cloud file in the underlying store.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="enableCache">Enables implicit, on-demand caching of instance value. Defaults to false.</param>
    static member New(values : seq<'T>, ?directory : string, ?serializer : ISerializer, ?enableCache : bool) : Local<CloudSequence<'T>> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory = defaultArg directory config.DefaultDirectory
        let! _serializer = local {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        let deserializer = serializer |> Option.map (fun ser stream -> ser.SeqDeserialize<'T>(stream, leaveOpen = false))
        let path = config.FileStore.GetRandomFilePath directory
        let writer (stream : Stream) = async {
            return _serializer.SeqSerialize<'T>(stream, values, leaveOpen = false) |> int64
        }
        let! etag, length = ofAsync <| config.FileStore.Write(path, writer)
        return new CloudSequence<'T>(path, etag, Some length, deserializer, ?enableCache = enableCache)
    }

    /// <summary>
    ///     Creates a collection of partitioned cloud sequences by persisting provided sequence as cloud files in the underlying store.
    ///     A new partition will be appended to the collection as soon as the 'maxPartitionSize' is exceeded in bytes.
    /// </summary>
    /// <param name="values">Input sequence.</param>
    /// <param name="maxPartitionSize">Maximum size in bytes per cloud sequence partition.</param>
    /// <param name="directory">FileStore directory used for Cloud sequence. Defaults to execution context.</param>
    /// <param name="serializer">Serializer used in sequence serialization. Defaults to execution context.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    static member NewPartitioned(values : seq<'T>, maxPartitionSize : int64, ?directory : string, ?serializer : ISerializer, ?enableCache : bool) : Local<CloudSequence<'T> []> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let directory = defaultArg directory config.DefaultDirectory
        let! _serializer = local {
            match serializer with
            | None -> return! Cloud.GetResource<ISerializer> ()
            | Some s -> return s
        }

        let deserializer = serializer |> Option.map (fun ser stream -> ser.SeqDeserialize<'T>(stream, leaveOpen = false))
        return! ofAsync <| async {
            if maxPartitionSize <= 0L then return invalidArg "maxPartitionSize" "Must be greater that 0."

            let seqs = new ResizeArray<CloudSequence<'T>>()
            let currentStream = ref Unchecked.defaultof<Stream>
            let splitNext () = currentStream.Value.Position >= maxPartitionSize
            let partitionedValues = PartitionedEnumerable.ofSeq splitNext values
            for partition in partitionedValues do
                let path = config.FileStore.GetRandomFilePath directory
                let writer (stream : Stream) = async {
                    currentStream := stream
                    return _serializer.SeqSerialize<'T>(stream, partition, leaveOpen = false) |> int64
                }
                let! etag, length = config.FileStore.Write(path, writer)
                let seq = new CloudSequence<'T>(path, etag, Some length, deserializer, ?enableCache = enableCache)
                seqs.Add seq

            return seqs.ToArray()
        }
    }

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="deserializer">Sequence deserializer function.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, ?deserializer : Stream -> seq<'T>, ?force : bool, ?enableCache : bool) : Local<CloudSequence<'T>> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let! etag = ofAsync <| config.FileStore.TryGetETag path
        match etag with
        | None -> return raise <| new FileNotFoundException(path)
        | Some et ->
            let cseq = new CloudSequence<'T>(path, et, None, deserializer, ?enableCache = enableCache)
            if defaultArg force false then
                let! _ = cseq.Count in ()

            return cseq
    }

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided serializer implementation.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to Cloud sequence.</param>
    /// <param name="serializer">Serializer implementation used for element deserialization.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, serializer : ISerializer, ?force : bool, ?enableCache) : Local<CloudSequence<'T>> = local {
        let deserializer stream = serializer.SeqDeserialize<'T>(stream, leaveOpen = false)
        return! CloudSequence.OfCloudFile<'T>(path, deserializer = deserializer, ?force = force, ?enableCache = enableCache)
    }

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided text deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="textDeserializer">Text deserializer function.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    static member OfCloudFile<'T>(path : string, textDeserializer : StreamReader -> seq<'T>, ?encoding : Encoding, ?force : bool, ?enableCache : bool) : Local<CloudSequence<'T>> = local {
        let deserializer (stream : Stream) =
            let sr = 
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)

            textDeserializer sr 
        
        return! CloudSequence.OfCloudFile(path, deserializer, ?force = force, ?enableCache = enableCache)
    }

    /// <summary>
    ///     Defines a CloudSequence from provided cloud file path with user-provided text deserialization function.
    ///     This is a lazy operation unless the optional 'force' parameter is enabled.
    /// </summary>
    /// <param name="path">Path to file.</param>
    /// <param name="encoding">Text encoding. Defaults to UTF8.</param>
    /// <param name="force">Check integrity by forcing deserialization on creation. Defaults to false.</param>
    /// <param name="enableCache">Enable caching by default on every node where cell is dereferenced. Defaults to false.</param>
    static member FromLineSeparatedTextFile(path : string, ?encoding : Encoding, ?force : bool, ?enableCache : bool) : Local<CloudSequence<string>> = local {
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        let! etag = ofAsync <| config.FileStore.TryGetETag path
        match etag with
        | None -> return raise <| new FileNotFoundException(path)
        | Some et ->
            let cseq = new TextLineSequence(path, et, ?encoding = encoding, ?enableCache = enableCache)
            if defaultArg force false then
                let! _ = cseq.Count in ()

            return cseq :> _
    }

    /// <summary>
    ///     Creates a copy of CloudSequence with updated cache behaviour.
    /// </summary>
    /// <param name="cacheByDefault">Cache behaviour to be set.</param>
    /// <param name="cseq">Input cloud sequence.</param>
    static member WithCacheBehaviour (cacheByDefault:bool) (cseq:CloudSequence<'T>) : CloudSequence<'T> = cseq.WithCacheBehaviour cacheByDefault