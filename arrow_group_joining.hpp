#ifndef ARROW_GROUP_JOINING
#define ARROW_GROUP_JOINING

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/acero/api.h>
#include <arrow/acero/exec_plan.h>

#include <iostream>
#include <vector>
#include <memory>

#include "utils.hpp"
#include "timer.h"

namespace cp = arrow::compute;
namespace fs = arrow::fs;
namespace ac = arrow::acero;


arrow::Status top_20_mean_desc(const std::shared_ptr<arrow::Table>& table)
{
    /* 
        the average cost of the 20 longest (in distance) 
        taxirides in January 2019 (descending)
    */

    // using acero
    // ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    // ac::OrderByNodeOptions order_opts{
    //     cp::Ordering{
    //         {cp::SortKey(arrow::FieldRef("trip_distance"), 
    //             cp::SortOrder::Descending)}
    //     }
    // };
    // ac::Declaration order("order_by", {std::move(source)}, std::move(order_opts));
    // ac::Declaration fetch(
    //     "fetch", {std::move(order)}, ac::FetchNodeOptions(0, 20));
    
    // auto agg_opts = 
    //     std::make_shared<cp::ScalarAggregateOptions>(
    //         cp::ScalarAggregateOptions::Defaults());
    // cp::Aggregate ag("mean", agg_opts, arrow::FieldRef("total_amount"));
    
    // ac::Declaration aggregate(
    //     "aggregate", {std::move(fetch)}, ac::AggregateNodeOptions({ag}));
    

    // {
    //     timer t;
    //     ARROW_ASSIGN_OR_RAISE(auto new_table, 
    //         ac::DeclarationToTable(std::move(aggregate)));
    //     std::cout << new_table->ToString() << std::endl;   
    // }
    // result is ~2s more then twice longer then pandas. Order by is a bottleneck
    
    // using arrow Datum with sort_indices
    // {
    //     timer t;        
    //     ARROW_ASSIGN_OR_RAISE(
    //         arrow::Datum datum, 
    //         cp::SortIndices(
    //             *(table->GetColumnByName("trip_distance")), 
    //             cp::ArraySortOptions(cp::SortOrder::Descending))
    //     );
        
    //     ARROW_ASSIGN_OR_RAISE(datum, cp::Take(table, datum));
                
    //     ARROW_ASSIGN_OR_RAISE(datum, 
    //         cp::Mean(
    //             datum.table()->GetColumnByName("total_amount")->Slice(0, 20)));

    //     std::cout << datum.ToString() << '\n';
    // }
    // result is nearly the same

    // using arrow Datum with rank
    {
        timer t;
        auto opts = std::make_shared<cp::RankOptions>(
            cp::SortOrder::Descending, 
            cp::NullPlacement::AtEnd, 
            cp::RankOptions::Tiebreaker::Dense
        );
                
        ARROW_ASSIGN_OR_RAISE(
            arrow::Datum datum, 
            cp::CallFunction(
                "rank",
                {table->GetColumnByName("trip_distance")}, 
                opts.get())
        );
        ARROW_ASSIGN_OR_RAISE(
            arrow::Datum filter,
            cp::CallFunction("less_equal", {datum,  arrow::Datum(20)})
        );
        ARROW_ASSIGN_OR_RAISE(
            datum,
            cp::Filter(table, filter)
        );

        ARROW_ASSIGN_OR_RAISE(datum, 
            cp::Mean(
                datum.table()->GetColumnByName("total_amount")));

        std::cout << datum.ToString() << '\n';
    }
    // another result greater 2s
    return arrow::Status::OK();
}

void run_main_ch_6_1()
{
    arrow::Status st;
    std::shared_ptr<arrow::Table> table;
    
    st = read_file_to_table(
        "../data/nyc_taxi_2019-01.csv", 
        table, 
        { 
            "passenger_count", "trip_distance", "total_amount"
        });
    st = top_20_mean_desc(table);
    
    std::cout << st.message() << "\n";
}


#endif