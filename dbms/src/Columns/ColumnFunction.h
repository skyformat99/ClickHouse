#pragma once

#include <Core/NamesAndTypes.h>
#include <Columns/IColumnDummy.h>

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;


namespace DB
{


/** A column containing a lambda expression.
  * Behaves like a constant-column. Contains an expression, but not input or output data.
  */
class ColumnFunction final : public COWPtrHelper<IColumnDummy, ColumnFunction>
{
private:
    friend class COWPtrHelper<IColumnDummy, ColumnFunction>;

public:
    ColumnFunction(size_t size, FunctionBasePtr function, const ColumnsWithTypeAndName & columns_to_capture);

    void appendArguments(const ColumnsWithTypeAndName & columns);
    ColumnWithTypeAndName reduce() const;

    const char * getFamilyName() const override { return "Function"; }

    MutableColumnPtr replicate(const Offsets & offsets) const override
    {
        if (size != offsets.size())
            throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        ColumnsWithTypeAndName replicated_capture;
        replicated_capture.reserve(captured_columns.size());

        for (const auto & column : captured_columns)
            replicated_capture.push_back({column.column->replicate(offsets), column.type, column.name});

        return ColumnFunction::create(size == 0 ? 0 : offsets.back(), function, replicated_capture);
    }

private:
    size_t size;
    FunctionBasePtr function;
    ColumnsWithTypeAndName captured_columns;

    void appendArgument(const ColumnWithTypeAndName & column);
};

}
