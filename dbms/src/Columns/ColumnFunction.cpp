#include <Interpreters/ExpressionActions.h>
#include <Columns/ColumnFunction.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnFunction::ColumnFunction(size_t size, FunctionBasePtr function, const ColumnsWithTypeAndName & columns_to_capture)
        : size(size), function(function)
{
    captured_columns.reserve(function->getArgumentTypes().size());
    for (const auto & column : columns_to_capture)
        appendArgument(column);

}

void ColumnFunction::appendArguments(const ColumnsWithTypeAndName & columns)
{
    for (const auto & column : columns)
        appendArgument(column);
}

void ColumnFunction::appendArgument(const ColumnWithTypeAndName & column)
{
    const auto & argumnet_types = function->getArgumentTypes();
    if (captured_columns.size() == argumnet_types.size())
        throw Exception("Cannot add argument " + std::to_string(argumnet_types.size() + 1) + "to ColumnFunction " +
                        "because all arguments are already captured.", ErrorCodes::LOGICAL_ERROR);

    auto index = captured_columns.size();
    if (!column.type->equals(*argumnet_types[index]))
        throw Exception("Cannot add argument " + std::to_string(argumnet_types.size() + 1) + "to ColumnFunction " +
                        "because it has incompatible type: got " + column.type->getName() +
                        ", but " + argumnet_types[index]->getName() + " is expected.", ErrorCodes::LOGICAL_ERROR);

    captured_columns.push_back(column);
}


ColumnWithTypeAndName ColumnFunction::reduce() const
{
    Block block(captured_columns);
    block.insert({nullptr, function->getReturnType(), ""});

    ColumnNumbers arguments(captured_columns.size());
    for (size_t i = 0; i < captured_columns.size(); ++i)
        arguments.push_back(i);

    function->execute(block, arguments, captured_columns.size());

    return block.getByPosition(captured_columns.size());
}

}
