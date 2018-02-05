#include <Storages/MergeTree/MergeTreeMutation.h>
#include <Storages/MergeTree/MergeTreeBlockInputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Parsers/formatAST.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/copyData.h>

#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeMutation::MergeTreeMutation(
    MergeTreeData & storage_, UInt32 version_, std::vector<MutationCommand> commands_)
    : storage(storage_), version(version_), commands(std::move(commands_))
    , log(&Logger::get(storage.getLogName() + " (Mutation " + toString(version) + ")"))
{
    for (const auto & cmd : commands)
    {
        LOG_TRACE(log, "MUTATION type: " << cmd.type << " predicate: " << cmd.predicate);
    }
}

void MergeTreeMutation::execute(const Context & context)
{
    /// TODO: add to in-progress mutations in storage
    MergeTreeData::DataPartsVector old_parts = storage.getDataPartsVector();
    MergeTreeData::DataPartsVector new_parts;
    try
    {
        for (const auto & old_part : old_parts)
        {
            auto new_part = executeOnPart(old_part, context);
            if (new_part)
                new_parts.push_back(storage.addPreCommittedPart(new_part));
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Rolling back the mutation");
        for (const auto & part : new_parts)
            storage.removePreCommittedPart(part);

        throw;
    }

    LOG_TRACE(log, "Committing " << new_parts.size() << " new parts.");
    storage.commitParts(new_parts);
}

MergeTreeData::MutableDataPartPtr MergeTreeMutation::executeOnPart(const MergeTreeData::DataPartPtr & part, const Context & context) const
{
    LOG_TRACE(log, "Executing on part " << part->name);

    MergeTreePartInfo new_part_info = part->info;
    new_part_info.version = version;

    String new_part_name;
    if (storage.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        new_part_name = new_part_info.getPartNameV0(part->getMinDate(), part->getMaxDate());
    else
        new_part_name = new_part_info.getPartName();

    MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(
        storage, new_part_name, new_part_info);
    new_data_part->partition.assign(part->partition);
    new_data_part->relative_path = "tmp_mut_" + new_part_name;
    new_data_part->is_temp = true;

    String new_part_tmp_path = new_data_part->getFullPath();

    Poco::File(new_part_tmp_path).createDirectories();

    NamesAndTypesList all_columns = storage.getColumnsList();

    BlockInputStreamPtr in = std::make_shared<MergeTreeBlockInputStream>(
        storage, part, DEFAULT_MERGE_BLOCK_SIZE, 0, 0, all_columns.getNames(),
        MarkRanges(1, MarkRange(0, part->marks_count)),
        false, nullptr, String(), true, 0, DBMS_DEFAULT_BUFFER_SIZE, false);

    for (const MutationCommand & cmd : commands)
    {
        if (cmd.type == MutationCommand::DELETE)
        {
            auto predicate_expr = ExpressionAnalyzer(cmd.predicate, context, nullptr, all_columns)
                .getActions(false);
            String col_name = cmd.predicate->getColumnName();

            /// TODO: this is the inverse to DELETE (leave only rows that satisfy the predicate).
            in = std::make_shared<FilterBlockInputStream>(in, predicate_expr, col_name);
        }
        else
            throw Exception("Unsupported mutation cmd type: " + toString(static_cast<int>(cmd.type)),
                ErrorCodes::LOGICAL_ERROR);
    }

    auto compression_settings = context.chooseCompressionSettings(0, 0); /// TODO
    MergedBlockOutputStream out(storage, new_part_tmp_path, all_columns, compression_settings);

    in->readPrefix();
    out.writePrefix();

    while (Block block = in->read())
        out.write(block);

    in->readSuffix();
    out.writeSuffixAndFinalizePart(new_data_part); /// TODO: refactor so that this can be done in the ordinarywriteSuffix call.

    return new_data_part;
}

}
