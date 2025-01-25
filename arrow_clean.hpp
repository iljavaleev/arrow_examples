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


namespace cp = arrow::compute;
namespace fs = arrow::fs;
namespace ac = arrow::acero;


arrow::Status read_file_to_table(
    std::string path, 
    std::shared_ptr<arrow::Table>& res, 
    std::vector<std::string> string_null_values)
{
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(path));
    
    arrow::csv::ConvertOptions conv_opts;
    conv_opts.null_values = string_null_values;
    conv_opts.strings_can_be_null = true;
    conv_opts.include_columns = {"Plate ID", "Registration State", 
    "Vehicle Make", "Vehicle Color", "Violation Time", "Street Name"};

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
    ac::Declaration filter(
        "project",
        {std::move(source)},
        ac::ProjectNodeOptions(fields)
    );
    std::shared_ptr<arrow::Table> response_table;
    ARROW_ASSIGN_OR_RAISE(response_table, 
        ac::DeclarationToTable(std::move(filter)));
    
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

void run_main_ch_5()
{
    arrow::Status st;
    std::shared_ptr<arrow::Table> table;
    st = read_file_to_table("../data/nyc-parking-violations-2020.csv", table, {"", "NA"});
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

#endif