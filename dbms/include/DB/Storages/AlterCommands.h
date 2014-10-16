#pragma once

#include <DB/Core/NamesAndTypes.h>
#include <DB/Storages/ColumnDefault.h>

namespace DB
{

/// Операция из запроса ALTER (кроме манипуляции с PART/PARTITION). Добавление столбцов типа Nested не развернуто в добавление отдельных столбцов.
struct AlterCommand
{
	enum Type
	{
		ADD,
		DROP,
		MODIFY
	};

	Type type;

	String column_name;

	/// Для ADD и MODIFY - новый тип столбца.
	DataTypePtr data_type;

	ColumnDefaultType default_type{};
	ASTPtr default_expression{};

	/// Для ADD - после какого столбца добавить новый. Если пустая строка, добавить в конец. Добавить в начало сейчас нельзя.
	String after_column;


	/// одинаковыми считаются имена, если они совпадают целиком или name_without_dot совпадает с частью имени до точки
	static bool namesEqual(const String & name_without_dot, const DB::NameAndTypePair & name_type)
	{
		String name_with_dot = name_without_dot + ".";
		return (name_with_dot == name_type.name.substr(0, name_without_dot.length() + 1) || name_without_dot == name_type.name);
	}

	void apply(NamesAndTypesList & columns,
			   NamesAndTypesList & materialized_columns,
			   NamesAndTypesList & alias_columns,
			   ColumnDefaults & column_defaults) const;
};

class AlterCommands : public std::vector<AlterCommand>
{
public:
	void apply(NamesAndTypesList & columns,
			   NamesAndTypesList & materialized_columns,
			   NamesAndTypesList & alias_columns,
			   ColumnDefaults & column_defaults) const;
};

}
