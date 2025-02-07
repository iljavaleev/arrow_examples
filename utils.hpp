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


template <typename TYPE,
          typename = typename std::enable_if<arrow::is_number_type<TYPE>::value |
                                             arrow::is_boolean_type<TYPE>::value |
                                             arrow::is_temporal_type<TYPE>::value>::type>
arrow::Result<std::shared_ptr<arrow::Array>> GetArrayDataSample(
    const std::vector<typename TYPE::c_type>& values) {
  using ArrowBuilderType = typename arrow::TypeTraits<TYPE>::BuilderType;
  ArrowBuilderType builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(values.size()));
  ARROW_RETURN_NOT_OK(builder.AppendValues(values));
  return builder.Finish();
}

arrow::Status VehicleColorFunction(
    cp::KernelContext* ctx, const cp::ExecSpan& batch, cp::ExecResult* out) 
{
    
    const char* chars = batch[0].array.GetValues<char>(2);
    const int* idx = batch[0].array.GetValues<int>(1);
 
    arrow::TypedBufferBuilder<int64_t> len_builder;
    arrow::BufferBuilder string_builder;
    ARROW_RETURN_NOT_OK(len_builder.Reserve(batch.length+1));  
   
    int64_t strt = *idx++;
    int64_t end{};

    int64_t len_count=0;
    ARROW_RETURN_NOT_OK(len_builder.Append(len_count));

    for (int64_t i = 0; i < batch.length; ++i) 
    {
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
    
    std::cout << len_buffer->size() << std::endl; 

    arrow::ArrayData ad(
        arrow::utf8(), batch.length, {nullptr, len_buffer, string_buffer});
    out->value = std::make_shared<arrow::ArrayData>(ad);
    
    

    return arrow::Status::OK();
}


arrow::Status VehicleColorEx(std::shared_ptr<arrow::Table>& table) 
{
    const std::string name = "clean_color";
    auto func = 
    std::make_shared<cp::ScalarFunction>(name, cp::Arity::Unary(), vehicle_color_doc);
   
    cp::ScalarKernel kernel({arrow::utf8()}, 
        arrow::utf8(), VehicleColorFunction);
    
    kernel.mem_allocation = cp::MemAllocation::PREALLOCATE;
    kernel.null_handling = cp::NullHandling::INTERSECTION;

    ARROW_RETURN_NOT_OK(func->AddKernel(std::move(kernel)));

    auto registry = cp::GetFunctionRegistry();
    ARROW_RETURN_NOT_OK(registry->AddFunction(std::move(func)));

    
    auto color_column = table->GetColumnByName("Vehicle Color")->Slice(0, 10);
    
    ARROW_ASSIGN_OR_RAISE(auto res, cp::CallFunction(name, {color_column}));
    
    std::cout << res.chunked_array()->ToString() << std::endl;
   
    return arrow::Status::OK();
}

