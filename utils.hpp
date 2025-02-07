#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include <regex>

namespace cp = arrow::compute;
namespace fs = arrow::fs;
namespace ac = arrow::acero;

// Demonstrate registering a user-defined Arrow compute function outside of the Arrow
// source tree

namespace cp = ::arrow::compute;

const cp::FunctionDoc vehicle_color_doc
{
    "Regex replacement",
    "clean dirty data in Vehicle Color column",
    {"color"}
};


arrow::Status VehicleColorFunction(
    cp::KernelContext* ctx, const cp::ExecSpan& batch, cp::ExecResult* out) 
{
    
    const char* chars = batch[0].array.GetValues<char>(2);
    const int* idx = batch[0].array.GetValues<int>(1);
    auto null_count = batch[0].array.GetBuffer(0);

    arrow::TypedBufferBuilder<int32_t> len_builder;
    arrow::BufferBuilder string_builder;
    
    ARROW_RETURN_NOT_OK(len_builder.Reserve(batch.length+1));
    
    int64_t strt = *idx++;
    int64_t end{};

    int64_t len_count=0;
    ARROW_RETURN_NOT_OK(len_builder.Append(len_count));

    for (int64_t i = 0; i < batch.length; ++i) 
    {
        if (batch[0].array.IsNull(i))
        {
            ARROW_RETURN_NOT_OK(len_builder.Append(len_count));
            idx++;
            continue;
        }        
        end = *idx++;
        size_t n = end - strt;
        char color[n];
        std::strncpy(color, chars + strt, n);
        strt = end;
        
        if (std::regex_search(color, std::regex(
        "^[Ww]{1}[Hh]{0,1}?[Ii]{0,1}?([Tt]{1})?[Ee.]{0,2}?$",                              std::regex_constants::ECMAScript)))
        {
            n = 6;
            ARROW_RETURN_NOT_OK(string_builder.Append("WHITE", n));
        }
        else if (std::regex_search(color, std::regex(
            "^[Gg]{1}[Rr]{0,1}?[Ee]{0,1}?[Yy.]{0,2}$",
            std::regex_constants::ECMAScript)))
        {
            n = 5;
            ARROW_RETURN_NOT_OK(string_builder.Append("GREY", n));
        }
        else if (std::regex_search(color, std::regex(
            "^[Yy]{1}[Ee]{0,1}?[Ll]{0,2}?[Oo]{0,1}[Ww.]{0,2}$", 
            std::regex_constants::ECMAScript)))
        {
            n = 7;
            ARROW_RETURN_NOT_OK(string_builder.Append("YELLOW", n));
        }
        else if (std::regex_search(color, std::regex(
            "^[Bb]{1}[Ll]{0,1}?[Aa]{0,1}?[Cc]{0,1}[Kk.]{0,2}$",                              std::regex_constants::ECMAScript)))
        {
            n = 6;
            ARROW_RETURN_NOT_OK(string_builder.Append("BLACK", n));
        }
        else
        {
            ARROW_RETURN_NOT_OK(string_builder.Append(color, n));
        }
        len_count += n;
        ARROW_RETURN_NOT_OK(len_builder.Append(len_count));
    }

    ARROW_ASSIGN_OR_RAISE(auto len_buffer, len_builder.Finish(false));
    ARROW_ASSIGN_OR_RAISE(auto string_buffer, string_builder.Finish());
    
    arrow::ArrayData ad(
        arrow::utf8(), batch.length, {null_count, len_buffer, string_buffer});
    out->value = std::make_shared<arrow::ArrayData>(std::move(ad));
    
    return arrow::Status::OK();
}

#include <future>

arrow::Datum get_chunk(
    const std::shared_ptr<arrow::Array>& chunk, 
    const std::string& name)
{
    arrow::Datum res;
    auto maybe_datum = cp::CallFunction(name, {chunk});
    if (!maybe_datum.ok())
        return res;
    res = *maybe_datum;
    return res;
}

arrow::Status VehicleColorEx(std::shared_ptr<arrow::Table>& table) 
{
    const std::string name = "clean_color";
    auto func = 
    std::make_shared<cp::ScalarFunction>(
        name, cp::Arity::Unary(), vehicle_color_doc);
   
    cp::ScalarKernel kernel({arrow::utf8()}, 
        arrow::utf8(), VehicleColorFunction);
    
    kernel.mem_allocation = cp::MemAllocation::PREALLOCATE;
    kernel.null_handling = cp::NullHandling::INTERSECTION;

    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));

    auto registry = cp::GetFunctionRegistry();
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));

    
    auto color_column = table->GetColumnByName("Vehicle Color");
    arrow::ArrayVector chunks = color_column->chunks();
    
    // ARROW_ASSIGN_OR_RAISE(auto res, cp::CallFunction(name, {chunks.at(0)}));
    // chunks.at(0) = res.make_array();
    // std::cout << chunks.at(0)->ToString() << std::endl;
    std::vector<std::future<arrow::Datum>> futures(chunks.size());
    
    for (size_t i=0; i<chunks.size(); i++)
    {
        futures[i] = std::async(std::launch::async, get_chunk, chunks[i], name);
    }

    for (size_t i=0; i<chunks.size(); i++)
    {
        chunks[i] = futures[i].get().make_array();
    }
    std::cout << chunks.at(0)->ToString() << std::endl;
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
    return arrow::Status::OK();
}

