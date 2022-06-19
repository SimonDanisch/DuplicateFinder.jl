using DuplicateFinder
using Test

duplicates = find_duplicate_folders(@__DIR__)
to_delete = select_to_delete(duplicates)
# Sanity check, that every file/dir to delete has a duplicate
@test isempty(find_without_dup(to_delete, duplicates))
# delete_objects(to_delete)

test_path = joinpath(@__DIR__, "file-test")
isdir(test_path) && rm(test_path; recursive=true, force=true)
mkpath(test_path)

function generate_files(root, recursion=3; nunique_files=10, nfiles=50, nfolders=3, filesize=1024, unique_files = [rand(UInt8, filesize) for i in 1:nunique_files])
    for i in 1:nfolders
        dir = joinpath(root, "dir_$(i)")
        mkpath(dir)
        for j in 1:nfiles
            file = joinpath(dir, "file_$j")
            write(file, unique_files[mod1(j, nunique_files)])
        end
        if recursion > 0
            generate_files(dir, recursion - 1; nunique_files, nfiles, nfolders, filesize, unique_files)
        end
    end
end

generate_files(test_path, 3)
function n_files(nf, nfi, rec)
    res = (nf * nfi)
    if rec > 0
        for i in 1:nfi
            res += n_files(nf, nfi, rec-1)
        end
    end
    return res
end
nfiles = DuplicateFinder.all_subs(test_path)
@test length(nfiles) == n_files(50, 3, 3)


duplicates = find_duplicate_files(test_path)
# there should only be 10 unique files in any folder
@test length(duplicates) == 10
to_delete = select_to_delete(duplicates)
@test length(to_delete) == length(nfiles) - 10
@test isempty(find_without_dup(to_delete, duplicates))
delete_objects(to_delete)

# Try finding again
duplicates = find_duplicate_files(test_path)
@test isempty(duplicates)
# Only 10 should be left
@test length(DuplicateFinder.all_subs(test_path)) == 10
rm(test_path; force=true, recursive=true)
