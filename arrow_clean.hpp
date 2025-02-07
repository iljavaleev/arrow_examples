#ifndef ARROW_CLEAN_HPP
#define ARROW_CLEAN_HPP

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/acero/api.h>
#include <arrow/acero/exec_plan.h>

#include <iostream>
#include <vector>

#include "utils.hpp"


namespace cp = arrow::compute;
namespace fs = arrow::fs;
namespace ac = arrow::acero;


arrow::Status read_file_to_table(
    std::string path, 
    std::shared_ptr<arrow::Table>& res,
    const std::vector<std::string>& include_columns,
    const std::vector<std::string>& string_null_values = {""})
{
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(path));
    
    arrow::csv::ConvertOptions conv_opts;
    conv_opts.null_values = string_null_values;
    conv_opts.strings_can_be_null = true;
    conv_opts.include_columns = include_columns;

    ARROW_ASSIGN_OR_RAISE(auto csv_reader, arrow::csv::TableReader::Make(
        arrow::io::default_io_context(), infile, 
        arrow::csv::ReadOptions::Defaults(),
        arrow::csv::ParseOptions::Defaults(), conv_opts));
    
    ARROW_ASSIGN_OR_RAISE(res, csv_reader->Read());
    return arrow::Status::OK();
}

arrow::Status drop_all_nan_ex(const std::shared_ptr<arrow::Table>& table)
{   
    
    size_t vsego = table->num_rows();
    std::cout << "Total rows: " << vsego << '\n';
    
    ARROW_ASSIGN_OR_RAISE(auto temp, cp::DropNull(table));
    size_t without_nulls = vsego - temp.length();
    std::cout << "Rows without any nans: " << without_nulls << '\n';

    return arrow::Status::OK();
}


arrow::Status drop_nan_in_subset(
    const std::shared_ptr<arrow::Table>& table, 
    const std::shared_ptr<std::vector<std::string>>& subset = NULL)
{   
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};

    std::vector<cp::Expression> fields;
    for (const auto& f: *subset)
    {
        fields.push_back(cp::field_ref(f));
    }  
    ac::Declaration project(
        "project",
        {std::move(source)},
        ac::ProjectNodeOptions(fields)
    );
    std::shared_ptr<arrow::Table> response_table;
    ARROW_ASSIGN_OR_RAISE(response_table, 
        ac::DeclarationToTable(std::move(project)));
    
    ARROW_ASSIGN_OR_RAISE(auto temp, cp::DropNull(response_table));
    size_t without_nulls = table->num_rows() - temp.length();
    std::cout << "Rows without nans in subset: " << without_nulls << '\n';
    return arrow::Status::OK();
}

arrow::Status get_filter(const std::shared_ptr<arrow::Table>& table, const std::shared_ptr<std::vector<std::string>>& subset)
{   
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    
    std::vector<cp::Expression> project_ex;
    std::vector<std::string> project_name;
    auto cast_opts = cp::CastOptions();
    cast_opts.to_type = arrow::int64();
    for (const auto& f: *subset)
    {
        project_name.push_back(f);
        project_ex.push_back(
            cp::call("cast", {
                cp::call("invert", {cp::is_null(cp::field_ref(f))})
            }, cast_opts)
        );
    }
    
    ac::Declaration project(
        "project",
        {std::move(source)},
        ac::ProjectNodeOptions(project_ex, project_name)
    );
    std::shared_ptr<arrow::Table> response_table;
    ARROW_ASSIGN_OR_RAISE(response_table, ac::DeclarationToTable(std::move(project)));

    int n = response_table->columns().size();
    arrow::Datum d(response_table->column(0));
    for (int i = 1; i<n; ++i)
    {
        ARROW_ASSIGN_OR_RAISE(d, cp::Add(d, response_table->column(i)));
    }
    arrow::Datum t = arrow::Int64Scalar(3);
    ARROW_ASSIGN_OR_RAISE(d, cp::CallFunction("greater_equal", {d, t}));
    
    arrow::Datum tab{table};
    auto res = cp::Filter(tab, d);
    std::cout << res->table()->num_rows();
    
    return arrow::Status::OK();
}


arrow::Status at_least_3_not_nulls(const std::shared_ptr<arrow::Table>& table, 
    const std::shared_ptr<std::vector<std::string>>& subset)
{   
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    
    std::vector<cp::Expression> project_ex;
    std::vector<std::string> project_name;
    auto cast_opts = cp::CastOptions();
    cast_opts.to_type = arrow::int64();
    
    for (const auto& f: *subset)
    {
        project_name.push_back(f);
        project_ex.push_back(
            cp::call("cast", {
                cp::call("invert", {cp::is_null(cp::field_ref(f))})
            }, cast_opts)
        );
    }
    
    cp::Expression summ = cp::field_ref(project_name.at(0));
    for (int i=1; i<subset->size(); ++i)
    {
        summ = cp::call("add", { summ, cp::field_ref(project_name.at(i))});
    }


    ac::Declaration plan = ac::Declaration::Sequence(
        {
            std::move(source),
            {"project", ac::ProjectNodeOptions(project_ex, project_name)},
            {"project", ac::ProjectNodeOptions({summ}, {"filter"})},
            {"project", ac::ProjectNodeOptions(
                    {   
                        cp::call("greater_equal", 
                        {cp::field_ref("filter"), cp::literal(3)})
                    }
                )
            }
        }
    );

    
    std::shared_ptr<arrow::Table> response_table;
    ARROW_ASSIGN_OR_RAISE(response_table, ac::DeclarationToTable(std::move(plan)));
    arrow::Datum input{table}, mask{response_table->column(0)};
    
    auto res = cp::Filter(input, mask);
    std::cout << table->num_rows() - res->table()->num_rows() << '\n';

    return arrow::Status::OK();
}


arrow::Status column_null_count(const std::shared_ptr<arrow::Table>& table)
{   
    for (size_t i=0; i<table->num_columns(); i++)
    {
        std::cout <<  table->schema()->field(i)->name() << ": " 
            <<  table->column(i)->null_count() << "\n";
    }
    
    return arrow::Status::OK();
}

arrow::Status plate_id(const std::shared_ptr<arrow::Table>& table)
{
    auto col = table->GetColumnByName("Plate ID");
    size_t null_count = col->null_count();
    arrow::Datum column{std::move(col)};
    
    arrow::Datum target_string{"BLANKPLATE"};
    auto mask = cp::CallFunction("equal", {column, target_string}); 
    ARROW_ASSIGN_OR_RAISE(column, cp::Cast(*mask, arrow::int64()));
    ARROW_ASSIGN_OR_RAISE(auto res, cp::Sum(column));

    std::cout << "New null count: "
        << res.scalar_as<arrow::Int64Scalar>().value + null_count << "\n";
    return arrow::Status::OK();
}

void run_main_ch_5_1()
{
    arrow::Status st;
    std::shared_ptr<arrow::Table> table;

    st = read_file_to_table(
        "../data/nyc-parking-violations-2020.csv", 
        table, 
        /*include_columns*/{"Plate ID", "Registration State", "Vehicle Make", 
        "Vehicle Color", "Violation Time", "Street Name"},
        /*null_values*/{"", "NA"});
    // auto subset = std::make_shared<std::vector<std::string>>(std::vector<std::string>{"Plate ID", "Registration State", "Vehicle Make", "Street Name"});
    // st = drop_all_nan_ex(table);
    // st = drop_nan_in_subset(table, subset);
    // std::cout << "Table with at least 3 not nulls in subset: ";
    // st = at_least_3_not_nulls(table, subset);
    // std::cout << "Column name: null count " << '\n';
    // st = column_null_count(table);
    // st = read_file_to_table("../data/nyc-parking-violations-2020.csv", table, {"", "NA", "BLANKPLATE"});
    // std::cout << "NEW Column name: null count " << '\n';
    // st = column_null_count(table);
    st = plate_id(table);
    
    std::cout << st.message();
}


//////////// 2 part

arrow::Status add_new_column(std::shared_ptr<arrow::Table>& table)
{
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    cp::Expression month = cp::call("month", {cp::field_ref("dateofdeath")});

    std::vector<cp::Expression> project_exs;
    std::vector<std::string> project_names;
   
    for (const auto& f: table->ColumnNames())
    {
        project_names.push_back(f);
        project_exs.push_back(cp::field_ref(f));
    }
    project_names.push_back("month");
    project_exs.push_back(std::move(month));

    ac::Declaration project = {
        "project",
        {std::move(source)},
        ac::ProjectNodeOptions(project_exs, project_names)
    };
    
    ARROW_ASSIGN_OR_RAISE(table, 
        ac::DeclarationToTable(std::move(project)));

    return arrow::Status::OK();
}


arrow::Status clean_age_column(std::shared_ptr<arrow::Table>& table)
{
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    
    auto opts = 
        std::make_shared<cp::ReplaceSubstringOptions>("[a-zA-Z/ ._-]*", "");
    cp::Expression subs = 
        cp::call("replace_substring_regex", {cp::field_ref("age")}, opts);
    
    ac::Declaration project
    {
        "project",
        {std::move(source)},
        ac::ProjectNodeOptions(
            {cp::field_ref("age"), cp::field_ref("dateofdeath"), subs}, 
            {"age", "dateofdeath", "age_nums"}
        )
    }; 
    
    auto r_opts = std::make_shared<cp::ReplaceSliceOptions>(2, 999, "");
    auto replace = cp::call("utf8_replace_slice", 
                {cp::field_ref("age_nums")}, r_opts);
    
    
    auto cast_opts = cp::CastOptions();
    cast_opts.to_type = arrow::int64();

    auto cast = cp::call("cast", {replace}, cast_opts);
    ac::Declaration project_out
    {
        "project",
        {std::move(project)},
        ac::ProjectNodeOptions(
            {cp::field_ref("dateofdeath"), cast}, 
            {"dateofdeath", "age"}
        )
    }; 

    ARROW_ASSIGN_OR_RAISE(table, 
        ac::DeclarationToTable(std::move(project_out)));

    std::cout << "Age mean: " << cp::Mean(
        table->GetColumnByName("age")).ValueOrDie().
            scalar_as<arrow::DoubleScalar>().value 
        << '\n';
    return arrow::Status::OK();
}


arrow::Status mean_age_in_period(std::shared_ptr<arrow::Table>& table)
{
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    ARROW_ASSIGN_OR_RAISE(auto start, arrow::TimestampScalar::FromISO8601(
        "2016-02-15", arrow::TimeUnit::SECOND));
    ARROW_ASSIGN_OR_RAISE(auto end, arrow::TimestampScalar::FromISO8601(
        "2016-07-15", arrow::TimeUnit::SECOND));
    
    auto lower_bound = cp::call("greater_equal", 
        {cp::field_ref("dateofdeath"), cp::literal(start)});
    auto upper_bound = cp::call("less_equal", 
        {cp::field_ref("dateofdeath"), cp::literal(end)});

    ac::Declaration filter
    {
        "filter",
        {std::move(source)},
        ac::FilterNodeOptions(
            cp::call("and", {lower_bound, upper_bound})
        )
    };

    
    ARROW_ASSIGN_OR_RAISE(auto new_table, 
        ac::DeclarationToTable(std::move(filter)));


    std::cout << "Age mean from feb to jul: " << cp::Mean(
        new_table->GetColumnByName("age")).ValueOrDie().
            scalar_as<arrow::DoubleScalar>().value 
        << '\n';

    return arrow::Status::OK();
}


arrow::Status top_five_causeofdeath(
    std::shared_ptr<arrow::Table> table, bool null_to_unknown=false)
{   
    
    if (!null_to_unknown)
    {
        ARROW_ASSIGN_OR_RAISE(auto temp, cp::DropNull(table));
        table = temp.table();
    }
    
    ac::Declaration start{"table_source", ac::TableSourceNodeOptions{table}};
    if (null_to_unknown)
    {
        start = ac::Declaration
        {
            "project",
            {std::move(start)},
            ac::ProjectNodeOptions(
                {
                    cp::call("coalesce", 
                    {cp::field_ref("causeofdeath"), cp::literal("unknown")}),
                    cp::field_ref("age")
                },
                {
                    "causeofdeath", "age"
                }
            )
        };
    }
    
    auto options = 
        std::make_shared<cp::CountOptions>(
            cp::CountOptions::CountMode::ALL);

    auto aggregate_options =
        ac::AggregateNodeOptions{
            {{"hash_count", options, "age", "count"}},
            {"causeofdeath"}}; //group by
    ac::Declaration aggregate{
        "aggregate", {std::move(start)}, std::move(aggregate_options)};
    
    ac::OrderByNodeOptions opts{
        cp::Ordering{
            {cp::SortKey(arrow::FieldRef("count"), cp::SortOrder::Descending)}
        }
    };
    ac::Declaration order("order_by", {std::move(aggregate)}, std::move(opts));
    
    ARROW_ASSIGN_OR_RAISE(auto new_table, 
        ac::DeclarationToTable(std::move(order)));
    
    auto r_opts = std::make_shared<cp::ReplaceSubstringOptions>("^ ", "");
    ac::Declaration plan = ac::Declaration::Sequence(
    {   
        {"table_source", ac::TableSourceNodeOptions{new_table->Slice(0, 5)}},
        {"project", ac::ProjectNodeOptions(
            {
                cp::call("replace_substring_regex", 
                    {cp::field_ref("causeofdeath")}, r_opts),
                cp::field_ref("count")
            },
            {"causeofdeath", "count"})   
        }
    });
    
    ARROW_ASSIGN_OR_RAISE(new_table, 
        ac::DeclarationToTable(std::move(plan)));
    std::cout << "Top five cause of death:\n" << 
        new_table->ToString() << std::endl;


    return arrow::Status::OK();
}


void run_main_ch_5_2()
{
    arrow::Status st;
    std::shared_ptr<arrow::Table> table;
   
    // st = read_file_to_table(
    //     "../data/celebrity_deaths_2016.csv", 
    //     table, 
    //     /*include_columns*/{"dateofdeath", "age"});
    // st = add_new_column(table); 
    // st = clean_age_column(table);
    // st = mean_age_in_period(table);
    
    st = read_file_to_table(
        "../data/celebrity_deaths_2016.csv", 
        table, 
        /*include_columns*/{"dateofdeath", "age", "causeofdeath"});
    st = top_five_causeofdeath(table);
    st = top_five_causeofdeath(table, true);


    std::cout <<  st.message();
}

//////////// 3 part

arrow::Status columns_includes_nulles(std::shared_ptr<arrow::Table>& table)
{
    std::cout << "Column name: null count" << "\n";
    for (size_t i=0; i<table->num_columns(); ++i)
    {
        
        auto null_cnt = table->column(i)->null_count();
        if (null_cnt)
            std::cout << table->schema()->field_names().at(i) 
                << ": " << null_cnt << "\n";
        else
            ARROW_RETURN_NOT_OK(table->RemoveColumn(i));
    }
    
    return arrow::Status::OK();
}

/*
    unique values from the embarked column and the values are the most common
    destination for each value of embarked
*/
arrow::Status embarked_most_common_destinations(std::shared_ptr<arrow::Table>& table)
{  
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    
    /* find mean value of age column */
    ARROW_ASSIGN_OR_RAISE(arrow::Datum datum, 
        cp::Mean(table->GetColumnByName("age")));
    ARROW_ASSIGN_OR_RAISE(datum, 
        cp::Cast(datum, cp::CastOptions::Unsafe(arrow::int64())));
    auto age_new_value = cp::literal(std::move(datum).scalar());
    
    /* find mean value of fare column among values less then 400 */
    ARROW_ASSIGN_OR_RAISE(datum, cp::CallFunction("less", 
        {table->GetColumnByName("fare"), arrow::Datum(400)}));
    ARROW_ASSIGN_OR_RAISE(datum, 
        cp::Filter(table->GetColumnByName("fare"), datum));
    ARROW_ASSIGN_OR_RAISE(datum, cp::Mean(datum));
    ARROW_ASSIGN_OR_RAISE(datum, 
        cp::Cast(datum, cp::CastOptions::Unsafe(arrow::int64())));
    auto fare_new_value = cp::literal(std::move(datum).scalar());

    /* map columns with values found earlier */
    auto start = ac::Declaration
    {
        "project",
        {std::move(source)},
        ac::ProjectNodeOptions(
            {
                cp::call("coalesce", {cp::field_ref("age"), age_new_value}),
                cp::call("coalesce", {cp::field_ref("fare"), fare_new_value}), 
                cp::field_ref("embarked"),
                cp::field_ref("home.dest")
            },
            {
               "age", "fare", "embarked", "home.dest"
            }
        )
    };
    /* filter table by home.dest not null */
    auto filter_nulls = ac::Declaration
    {
        "filter",
        {start},
        ac::FilterNodeOptions(
            cp::call("invert", 
                {cp::call("is_null", {cp::field_ref("home.dest")})}
            )
        )
    };
    
    /* count values grop by "embarked", "home.dest" columns */
    std::vector<arrow::FieldRef> fields_to_agg = {"age"};
    std::vector<cp::Aggregate> aggs;
    aggs.emplace_back(
        "hash_count", 
        std::make_shared<cp::CountOptions>(), 
        std::move(fields_to_agg), 
        "count");
    auto aggregate_options =
        ac::AggregateNodeOptions{std::move(aggs), {"embarked", "home.dest"}};
    ac::Declaration aggregate1{
        "aggregate", {std::move(filter_nulls)}, std::move(aggregate_options)};
    
    /* find max values for distinct "embarked" values */
    fields_to_agg = {"count"};
    aggs.emplace_back("hash_max", 
        std::make_shared<cp::ScalarAggregateOptions>(), 
        std::move(fields_to_agg), 
        "max");
    aggregate_options = ac::AggregateNodeOptions{std::move(aggs), {"embarked"}};
    ac::Declaration aggregate2{
        "aggregate", {aggregate1}, std::move(aggregate_options)};

    /* join tables to get home.dest column */
    ac::HashJoinNodeOptions join_opts{arrow::acero::JoinType::INNER,
        {"embarked", "count"}, 
        {"embarked", "max"}, 
        cp::literal(true), "_l", "_r"};

    ac::Declaration join
    {
        "hashjoin",
        {std::move(aggregate1), std::move(aggregate2)}, join_opts
    };

    /* get final results */
    auto end_first_part = ac::Declaration
    {
        "project",
        {std::move(join)},
        ac::ProjectNodeOptions(
            {
                
                cp::field_ref("embarked_l"),
                cp::field_ref("home.dest"),
                cp::field_ref("count"),
            },
            {
               "embarked", "home.dest", "count"
            }
        )
    };

    /* Replace null values in the home.dest column with values from embarked */
    auto second_part = ac::Declaration
    {
        "project",
        {std::move(start)},
        ac::ProjectNodeOptions(
            {
                cp::call("coalesce", {
                    cp::field_ref("home.dest"), cp::field_ref("embarked")})
            },
            {
                "home.dest.embarked"
            }
        )
    };

     /* join tables on embarked column */
    ac::HashJoinNodeOptions join_opts_1{arrow::acero::JoinType::LEFT_OUTER,
        {"home.dest.embarked"}, 
        {"embarked"}, 
        cp::literal(true), "_l", "_r"};

    ac::Declaration join_1
    {
        "hashjoin",
        {std::move(second_part), std::move(end_first_part)}, join_opts_1
    };

    auto third_part_end = ac::Declaration
    {
        "project",
        {std::move(join_1)},
        ac::ProjectNodeOptions(
            {
                cp::call("coalesce", 
                {
                    cp::field_ref("home.dest"),
                    cp::field_ref("home.dest.embarked")
                })
            },
            {
                "home.dest.without_nulls"
            }
        )
    };

    ARROW_ASSIGN_OR_RAISE(auto new_table, 
        ac::DeclarationToTable(std::move(third_part_end)));
    std::cout << "Home destinations based on embarked columncd:\n" << 
        new_table->ToString() << std::endl;

    return arrow::Status::OK();
}



void run_main_ch_5_3()
{
    arrow::Status st;
    std::shared_ptr<arrow::Table> table;
    

    st = read_file_to_table(
        "../data/titanic3.csv", 
        table, {});
    // std::cout << table->ToString();
    st = columns_includes_nulles(table);
    st = embarked_most_common_destinations(table);
    std::cout <<  st.message();
}

//////////// 3 part


arrow::Status unique_values_len(
    const std::shared_ptr<arrow::Table>& table,
    std::string&& column_name)
{
    auto column = table->GetColumnByName(column_name);
    ARROW_ASSIGN_OR_RAISE(auto res, cp::Unique(column));
    std::cout << res->length() << std::endl;
    return arrow::Status::OK();
}

arrow::Status top_30(
    const std::shared_ptr<arrow::Table>& table,
    std::string&& column_name)
{
    auto column = table->GetColumnByName(column_name);
    ARROW_ASSIGN_OR_RAISE(auto counted, cp::ValueCounts(column));
    
    arrow::compute::SortOptions so;
    so.sort_keys = {
        arrow::compute::SortKey{
            "counts", 
            arrow::compute::SortOrder::Descending
        }
    };
    
    // array of sorted indices
    ARROW_ASSIGN_OR_RAISE(
        arrow::Datum res, 
        cp::CallFunction("sort_indices", {counted}, &so)
    );

    // sorted table
    ARROW_ASSIGN_OR_RAISE(
        arrow::Datum sorted, 
        cp::CallFunction("take", {counted, std::move(res)})
    );
    
    ARROW_ASSIGN_OR_RAISE(auto output, 
        cp::DropNull(sorted.array_as<arrow::StructArray>()->field(0)));
    
    auto result = output.array_as<arrow::StringArray>();
    std::cout << result->Slice(0, 30)->ToString() << std::endl;
    return arrow::Status::OK();
}


arrow::Status clean_column_with_regex(std::shared_ptr<arrow::Table>& table)
{
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    std::vector<std::shared_ptr<cp::ReplaceSubstringOptions>> vopts = 
    {
        std::make_shared<cp::ReplaceSubstringOptions>(
            "^[Ww]{1}[Hh]{0,1}?[Ii]{0,1}?([Tt]{1})?[Ee.]{0,2}?$", "WHITE", 1),
        std::make_shared<cp::ReplaceSubstringOptions>(
            "^[Gg]{1}[Rr]{0,1}?[Ee]{0,1}?[Yy.]{0,2}$", "GREY", 1),
        std::make_shared<cp::ReplaceSubstringOptions>(
            "^[Yy]{1}[Ee]{0,1}?[Ll]{0,2}?[Oo]{0,1}[Ww.]{0,2}$", "YELLOW", 1),
        std::make_shared<cp::ReplaceSubstringOptions>(
            "^[Bb]{1}[Ll]{0,1}?[Aa]{0,1}?[Cc]{0,1}[Kk.]{0,2}$", "BLACK", 1),
    };
    cp::Expression swap = cp::field_ref("Vehicle Color");
    for (int i=0; i<vopts.size(); ++i)
    {
        swap = cp::call(
            "replace_substring_regex", { swap }, vopts.at(i));
    }

    std::vector<cp::Expression> project_exs;
    std::vector<std::string> project_names;
   
    for (const auto& f: table->ColumnNames())
    {
        if (f == "Vehicle Color")
            continue;
        project_names.push_back(f);
        project_exs.push_back(cp::field_ref(f));
    }
    project_exs.push_back(std::move(swap));
    project_names.push_back("Vehicle Color");
    ac::Declaration project
    {
        "project",
        {std::move(source)},
        ac::ProjectNodeOptions(
            {project_exs}, 
            {project_names}
        )
    }; 

    ARROW_ASSIGN_OR_RAISE(table, 
        ac::DeclarationToTable(std::move(project)));
   
    return arrow::Status::OK();
}


void run_main_ch_5_4()
{
    arrow::Status st;
    std::shared_ptr<arrow::Table> table;
    

    st = read_file_to_table(
        "../data/nyc-parking-violations-2020.csv", 
        table, 
        { 
            "Plate ID", "Registration State", "Vehicle Make", "Vehicle Color", 
            "Street Name"
        });
    // std::cout << "Unique values in Vehicle Color column: ";
    // st = unique_values_len(table, "Vehicle Color");
    // std::cout << "Top 30 colors: ";
    // st = top_30(table, "Vehicle Color");
    // st = clean_column_with_regex(table);
    // st = top_30(table, "Vehicle Color");
    st = VehicleColorEx(table);
    std::cout <<  st.message();
}



#endif