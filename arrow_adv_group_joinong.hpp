#ifndef ARROW_ADV_GROUP_JOINING_HPP
#define ARROW_ADV_GROUP_JOINING_HPP

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/acero/api.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/dataset/api.h>

#include <iostream>
#include <vector>
#include <unistd.h>
#include <algorithm>


#include "clean_utils.hpp"
#include "utils.hpp"

namespace cp = arrow::compute;
namespace fs = arrow::fs;
namespace ac = arrow::acero;
namespace ds = arrow::dataset;


arrow::Status get_city_state(std::vector<std::string>& res, 
    const std::string& path)
{
    size_t start = path.rfind('/');
    size_t end = path.rfind('.');
    size_t middle = path.rfind(',');
    
    if (start == std::string::npos || end == std::string::npos || 
        middle == std::string::npos)
        return arrow::Status::RError("Error path");
    
    std::string city = path.substr(start + 1, middle - start - 1);
    size_t plus;
    if ((plus = city.find("+")) != std::string::npos)
        city.at(plus) = ' ';
    std::string state = path.substr(middle + 1, end - middle - 1);
    
    res.push_back(std::move(city));
    res.push_back(std::move(state));
    
    return arrow::Status::OK();
}

arrow::Status get_dataset(
    const std::shared_ptr<ds::Dataset>& ds, 
    std::shared_ptr<arrow::Table>& result)
{
    std::shared_ptr<arrow::fs::FileSystem> fs;
    char init_path[256];
    char* pwd_path = getcwd(init_path, 256); 
    if (!pwd_path)
        return arrow::Status::IOError("Fetching PWD failed.");
    ARROW_ASSIGN_OR_RAISE(fs, arrow::fs::FileSystemFromUriOrPath(init_path));
    arrow::fs::FileSelector selector;
    selector.base_dir = "../data/weather";
    selector.recursive = true;
   
    arrow::dataset::FileSystemFactoryOptions options; 
    auto read_format = std::make_shared<arrow::dataset::CsvFileFormat>();
    ARROW_ASSIGN_OR_RAISE(
    auto factory, arrow::dataset::FileSystemDatasetFactory::Make(fs, selector,  
        read_format, options));

    ARROW_ASSIGN_OR_RAISE(auto read_dataset, factory->Finish());
    ARROW_ASSIGN_OR_RAISE(auto fragments, read_dataset->GetFragments());
    auto dataset_schema_fields = read_dataset->schema()->fields();
    arrow::FieldVector fragment_fields;
    std::vector<std::shared_ptr<arrow::Table>> tables;
    std::vector<std::string> city_state;
    for (const auto& fragment: fragments)
    {
        ARROW_RETURN_NOT_OK(get_city_state(city_state, (*fragment)->ToString()));

        auto scan_opts = std::make_shared<ds::CsvFragmentScanOptions>();
        cp::ExecContext exec_context;
        auto fr = (*fragment)->InspectFragment(scan_opts.get(), &exec_context);
        ARROW_ASSIGN_OR_RAISE(auto inspector, fr.MoveResult());
        std::vector<std::string> names = std::vector<std::string>(
            inspector->column_names.begin(), 
            inspector->column_names.begin() + 3);
        
        for (int j=0; j<inspector->column_names.size(); ++j)
        {
            fragment_fields.push_back(dataset_schema_fields.at(j)->WithName(
                inspector->column_names.at(j)));
        }
        auto fragment_schema = arrow::schema(std::move(fragment_fields));
        
        std::vector<cp::Expression> exprs;
        for (int i=0; i<names.size(); i++)
            exprs.push_back(cp::field_ref(names.at(i)));
            
        std::vector<std::string> new_names{
            "date_time", "max_temp", "min_temp", "city", "state"
        };

        arrow::csv::ConvertOptions conv_opts;
        scan_opts->convert_options = std::move(conv_opts);

        std::shared_ptr<ds::ScanOptions> scan_options = 
            std::make_shared<ds::ScanOptions>();
        scan_options->dataset_schema = fragment_schema;
        scan_options->fragment_scan_options = scan_opts;
        arrow::dataset::ScannerBuilder scan_builder(
            std::move(fragment_schema), *fragment, scan_options);
        
        
        exprs.push_back(cp::literal(city_state.at(0)));
        exprs.push_back(cp::literal(city_state.at(1)));
        city_state.clear();

        ARROW_RETURN_NOT_OK(scan_builder.Project(exprs, new_names)); 
        ARROW_ASSIGN_OR_RAISE(auto scanner, scan_builder.Finish());
        tables.push_back(scanner->ToTable().ValueOrDie());
    }

    ARROW_ASSIGN_OR_RAISE(result, arrow::ConcatenateTables(tables));
    return arrow::Status::OK();
}


void adv_group_joining_1()
{
    arrow::Status st;
    std::shared_ptr<ds::Dataset> ds;
    std::shared_ptr<arrow::Table> table;
    st = get_dataset(ds, table);
    std::cout << table->ToString() << std::endl;

    std::cout << st.message();
}


#endif