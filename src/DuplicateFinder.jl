module DuplicateFinder

using SHA, ProgressMeter

by_name_hash(file) = Vector{UInt8}(basename(file))

function by_val_hash(file)
    return open(file, "r") do io
        return SHA.sha1(io)
    end
end

sha_hash1(file) = bytes2hex(by_val_hash(file))

function all_subs(folder = @__DIR__; select=:files)
    select in (:files, :dirs) || error("Select needs to be :files/:dirs")
    allfiles = String[]
    for (root, dirs, files) in walkdir(folder)
        to_iterate = select === :files ? files : dirs
        for file in to_iterate
            full_path = joinpath(root, file)
            push!(allfiles, full_path)
        end
    end
    return allfiles
end

function gen_hashes(files)
    result = Dict{String, Vector{String}}()
    l = Threads.SpinLock()
    files_processed = Threads.Atomic{Int}(0)
    nfiles = length(files)
    Threads.@threads for full_path in files
        try
            key = sha_hash1(full_path)
            lock(l) do
                same_files = get!(result, key, String[])
                push!(same_files, full_path)
            end
        catch e
            @warn("skipping $(full_path)")
        end
        Threads.atomic_add!(files_processed, 1)
        x = files_processed[]
        if x % 1000 == 0
            println(round((x / nfiles) * 100, digits=2), "%")
        end
    end
    return result
end

function folder_hash!(hashes, folder, filehash=by_name_hash)
    return get!(hashes, folder) do
        ctx = SHA.SHA1_CTX()
        for file in readdir(folder; join=true)
            if isfile(file)
                SHA.update!(ctx, filehash(file))
            elseif isdir(file)
                hash = folder_hash!(hashes, file, filehash)
                SHA.update!(ctx, hash)
            else
                @warn("What is this? $(file)")
            end
        end
        return SHA.digest!(ctx)
    end
end

function folder_hashes(folder)
    hashes = Dict{String, Vector{UInt8}}()
    folder_hash!(hashes, folder)
    return hashes
end

function subdirs(folder)
    dirs = String[]
    for file in readdir(folder; join=true)
        isdir(file) && push!(dirs, file)
    end
    return dirs
end


"""
    remove_subfolders(dir2hash, hash2dirs)

Returns only parent folders, which are duplicate
"""
function remove_subfolders(dir2hash, hash2dirs)
    result_hash2dirs = copy(hash2dirs)
    for (dir, hash) in dir2hash
        dup_folders = hash2dirs[hash]
        for folder in dup_folders
            # remove all folders that are in folder from the potential duplicates
            # since any duplicate in there is already covered by the parent folder
            for subfolder in all_subs(folder; select=:dirs)
                hash2 = dir2hash[subfolder]
                delete!(result_hash2dirs, hash2)
            end
        end
    end
    return result_hash2dirs
end


"""
    filter_by_value_hash!(duplicates)
Filters out duplicates in duplicates, which don't have an exact file match
"""
function filter_by_value_hash!(duplicates)
    hashes = Dict{String, Vector{UInt8}}()
    for folders in duplicates
        if length(folders) > 1
            hash = folder_hash!(hashes, first(folders), by_val_hash)
            to_remove = Int[]
            for i in 2:length(folders)
                hash2 = folder_hash!(hashes, folders[i], by_val_hash)
                if hash != hash2
                    push!(to_remove, i)
                end
            end
            splice!(folders, to_remove)
        end
    end
    return filter!(x-> length(x) > 1, duplicates)
end

by_last_edit(path) = Base.Filesystem.mtime(path)

function select_to_delete(duplicates, method=by_last_edit)
    to_delete = String[]
    for folders in duplicates
        sort!(folders; by=method, rev=true)
        # keep first
        append!(to_delete, folders[2:end])
    end
    return to_delete
end

function find_without_dup(to_delete, all_dups)
    no_duplicate = String[]
    for dir in to_delete
        has_duplicate = false
        idx = findfirst(x-> dir in x, all_dups)
        if isnothing(idx) || length(all_dups[idx]) < 2
            push!(no_duplicate, dir)
        end
    end
    return no_duplicate
end

function delete_objects(files_or_folders)
    @showprogress for dir in files_or_folders
        try
            rm(dir; force=true, recursive=true)
        catch e
            @warn("cant delete $(dir)")
        end
    end
end

function hashdict_to_duplicates(hashes::Dict{String, Vector{UInt8}})
    duplicates = Dict{Vector{UInt8}, Vector{String}}()
    for (dir, hash) in hashes
        push!(get!(duplicates, hash, String[]), dir)
    end
    return duplicates
end

function find_duplicate_folders(folder; method=by_val_hash)
    # get a hash for every folder
    hashes = folder_hashes(folder)
    # get a dict hash -> [duplicates...]
    dup_hashes = hashdict_to_duplicates(hashes)
    # remove all duplicates, that are already covered by a parent folder that is a duplicate
    parent_dubs = remove_subfolders(hashes, dup_hashes)
    # get all duplicates
    unique_dups = filter(x-> length(x) > 1, collect(values(parent_dubs)))
    if method == by_val_hash
        # TODO, is it faster to just make `folder_hashes` use by_fal_hash?
        return filter_by_value_hash!(unique_dups)
    else
        return unique_dups
    end
end

function find_duplicate_files(folder)
    files = all_subs(folder)
    @info "found $(length(files)) files. Generating hashes!"
    hashes = gen_hashes(files)
    # find all duplicates
    duplicates = filter(files-> length(files) > 1, collect(values(hashes)))
    @info "found $(length(duplicates)) duplicates"
    return duplicates
end

export find_duplicate_folders, find_duplicate_files, delete_objects, select_to_delete, find_without_dup

end
