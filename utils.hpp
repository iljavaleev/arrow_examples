#ifndef UTILS_HPP
#define UTILS_HPP

#include <arrow/api.h>

#include <string>
#include <iostream>
#include <vector>


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

arrow::Status read_file_to_table_with_opts(
    std::string path, 
    std::shared_ptr<arrow::Table>& res,
    arrow::csv::ReadOptions read_opts = arrow::csv::ReadOptions::Defaults(),
    arrow::csv::ParseOptions parse_opts = arrow::csv::ParseOptions::Defaults(),
    arrow::csv::ConvertOptions conv_opts = arrow::csv::ConvertOptions::Defaults()
)
{
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(path));
    
    ARROW_ASSIGN_OR_RAISE(auto csv_reader, arrow::csv::TableReader::Make(
        arrow::io::default_io_context(), infile, read_opts, parse_opts, 
        conv_opts));
    
    ARROW_ASSIGN_OR_RAISE(res, csv_reader->Read());
    return arrow::Status::OK();
}


 // auto data = res.chunked_array()->chunk(0)->data();
    // auto db = data->buffers;
    // int32_t* idx = db.at(1)->mutable_data_as<int32_t>();
    // char* vals = db.at(2)->mutable_data_as<char>();
    // int32_t strt = *idx++;
    // int32_t end{};

    // for (int64_t i = 0; i < 10; ++i) 
    // {
    //     end = *idx++;
    //     std::cout << end << std::endl;
    //     for (int64_t j = strt; j < end; ++j)
    //         std::cout << vals[j];
    //     std::cout << std::endl;
    //     strt = end;
    // }


#endif