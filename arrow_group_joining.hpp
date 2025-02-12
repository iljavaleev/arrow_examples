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

    //using acero
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    ac::OrderByNodeOptions order_opts{
        cp::Ordering{
            {cp::SortKey(arrow::FieldRef("trip_distance"), 
                cp::SortOrder::Descending)}
        }
    };
    ac::Declaration order("order_by", {std::move(source)}, std::move(order_opts));
    ac::Declaration fetch(
        "fetch", {std::move(order)}, ac::FetchNodeOptions(0, 20));
    
    auto agg_opts = 
        std::make_shared<cp::ScalarAggregateOptions>(
            cp::ScalarAggregateOptions::Defaults());
    cp::Aggregate ag("mean", agg_opts, arrow::FieldRef("total_amount"));
    
    ac::Declaration aggregate(
        "aggregate", {std::move(fetch)}, ac::AggregateNodeOptions({ag}));
    

    {
        timer t;
        ARROW_ASSIGN_OR_RAISE(auto new_table, 
            ac::DeclarationToTable(std::move(aggregate)));
        std::cout << new_table->ToString() << std::endl;   
    }
    // result is ~2s more then twice longer then pandas. Order by is a bottleneck
    
    // using arrow Datum with sort_indices
    {
        timer t;        
        ARROW_ASSIGN_OR_RAISE(
            arrow::Datum datum, 
            cp::SortIndices(
                *(table->GetColumnByName("trip_distance")), 
                cp::ArraySortOptions(cp::SortOrder::Descending))
        );
        
        ARROW_ASSIGN_OR_RAISE(datum, cp::Take(table, datum));
                
        ARROW_ASSIGN_OR_RAISE(datum, 
            cp::Mean(
                datum.table()->GetColumnByName("total_amount")->Slice(0, 20)));

        std::cout << datum.ToString() << '\n';
    }
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


arrow::Status top_50_mean_pass_dist(const std::shared_ptr<arrow::Table>& table)
{
    /* 
       Sort by ascending passenger count and descending trip distance
        Average price paid for the top 50 rides
    */

    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    ac::OrderByNodeOptions order_opts{
        cp::Ordering{
            {
            cp::SortKey(arrow::FieldRef("passenger_count"), 
                cp::SortOrder::Ascending),
            cp::SortKey(arrow::FieldRef("trip_distance"), 
                cp::SortOrder::Descending)
            }
        }
    };
    ac::Declaration order("order_by", {std::move(source)}, std::move(order_opts));
    ac::Declaration fetch(
        "fetch", {std::move(order)}, ac::FetchNodeOptions(0, 50));
    
    auto agg_opts = 
        std::make_shared<cp::ScalarAggregateOptions>(
            cp::ScalarAggregateOptions::Defaults());
    cp::Aggregate ag("mean", agg_opts, arrow::FieldRef("total_amount"));
    
    ac::Declaration aggregate(
        "aggregate", {std::move(fetch)}, ac::AggregateNodeOptions({ag}));
    
    {
        timer t;
        ARROW_ASSIGN_OR_RAISE(auto new_table, 
            ac::DeclarationToTable(std::move(aggregate)));
        std::cout << new_table->ToString() << std::endl;   
    }
    return arrow::Status::OK();
}

arrow::Status top_5_per_mile(const std::shared_ptr<arrow::Table>& table)
{
    /* 
        In which five rides did people pay the most per mile? 
        How far did people go on those trips?
    */

    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    
    auto exp = cp::call("and", {
        cp::call("not_equal", {cp::field_ref("total_amount"), cp::literal(0)}),
        cp::call("not_equal", {cp::field_ref("trip_distance"), cp::literal(0)})
    });

    ac::Declaration filter{
        "filter", 
        {std::move(source)},
        ac::FilterNodeOptions(exp)
    };
    
    std::vector<cp::Expression> project_exs;
    std::vector<std::string> project_names;
    for (const auto& f: table->ColumnNames())
    {
        project_names.push_back(f);
        project_exs.push_back(cp::field_ref(f));
    }
    project_exs.push_back(
        cp::call("divide", 
            {cp::field_ref("total_amount"), cp::field_ref("trip_distance")}));
    project_names.push_back("cost_per_mile");
    
    ac::Declaration project{
        "project",
        {std::move(filter)},
        ac::ProjectNodeOptions(project_exs, project_names)
    };
    
    ac::OrderByNodeOptions order_opts{
        cp::Ordering{
            {
                cp::SortKey(arrow::FieldRef("cost_per_mile"), 
                cp::SortOrder::Descending)
            }
        }
    };

    ac::Declaration order("order_by", {std::move(project)}, std::move(order_opts));
    ac::Declaration fetch(
        "fetch", {std::move(order)}, ac::FetchNodeOptions(0, 5));
    
    ac::Declaration answ{
        "project",
        {std::move(fetch)},
        ac::ProjectNodeOptions(
            {cp::field_ref("trip_distance"), cp::field_ref("cost_per_mile")})
    };
    
    {
        timer t;
        ARROW_ASSIGN_OR_RAISE(auto new_table, 
            ac::DeclarationToTable(std::move(answ)));
        std::cout << new_table->ToString() << std::endl;   
    }
    return arrow::Status::OK();
    
}



arrow::Status top_10_per_pass(const std::shared_ptr<arrow::Table>& table)
{
    /* 
        Assume that multipassenger rides are split evenly among the passengers.
        In which 10 multipassenger rides did each individual pay the greatest amount?
    */

    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    
    auto exp = cp::call("and", {
        cp::call("not_equal", {cp::field_ref("total_amount"), cp::literal(0)}),
        cp::call("not_equal", {cp::field_ref("passenger_count"), cp::literal(0)})
    });

    ac::Declaration filter{
        "filter", 
        {std::move(source)},
        ac::FilterNodeOptions(exp)
    };
    
    std::vector<cp::Expression> project_exs;
    std::vector<std::string> project_names;
    for (const auto& f: table->ColumnNames())
    {
        project_names.push_back(f);
        project_exs.push_back(cp::field_ref(f));
    }
    project_exs.push_back(
        cp::call("divide", 
            {cp::field_ref("total_amount"), cp::field_ref("passenger_count")}));
    project_names.push_back("cost_per_passenger");
    
    ac::Declaration project{
        "project",
        {std::move(filter)},
        ac::ProjectNodeOptions(project_exs, project_names)
    };
    
    ac::OrderByNodeOptions order_opts{
        cp::Ordering{
            {
                cp::SortKey(arrow::FieldRef("cost_per_passenger"), 
                cp::SortOrder::Descending)
            }
        }
    };

    ac::Declaration order("order_by", {std::move(project)}, std::move(order_opts));
    ac::Declaration fetch(
        "fetch", {std::move(order)}, ac::FetchNodeOptions(0, 10));
    
    
    {
        timer t;
        ARROW_ASSIGN_OR_RAISE(auto new_table, 
            ac::DeclarationToTable(std::move(fetch)));
        std::cout << new_table->ToString() << std::endl;   
    }
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
    // st = top_20_mean_desc(table);
    // st = top_50_mean_pass_dist(table);
    // st = top_5_per_mile(table);
    st = top_10_per_pass(table);
    std::cout << st.message() << "\n";
}

// part 2


void run_main_ch_6_2()
{
    arrow::Status st;
    std::shared_ptr<arrow::Table> table;
    
    
    std::cout << st.message() << "\n";
}

#endif