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
arrow::Status mean_cost_taxi_ride(
    const std::shared_ptr<arrow::Table>& table, 
    const std::string&& sort_field,
    bool ascending=true
) 
{
    /*
        For each number of passengers, find the mean cost of a taxi ride. 
        Sort this result from lowest (i.e., cheapest) to highest 
        (i.e., most expensive), by number of passengers
    */
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    auto options = 
        std::make_shared<cp::ScalarAggregateOptions>(
            cp::ScalarAggregateOptions::Defaults());
    auto aggregate_options =
        ac::AggregateNodeOptions{
            {{"hash_mean", options, "total_amount", "mean"}},
            {"passenger_count"}};
    ac::Declaration aggregate{
        "aggregate", {std::move(source)}, std::move(aggregate_options)};
    
    cp::SortOrder order = cp::SortOrder::Descending;
    if (ascending)
        order = cp::SortOrder::Ascending;

    ac::OrderByNodeOptions opts{
        cp::Ordering{
            {cp::SortKey(arrow::FieldRef(sort_field), std::move(order))}
        }
    };
    ac::Declaration sort("order_by", {std::move(aggregate)}, std::move(opts));
    {
        timer t;
        ARROW_ASSIGN_OR_RAISE(auto new_table, 
            ac::DeclarationToTable(std::move(sort)));
        std::cout << new_table->ToString() << std::endl;   
    }
    return arrow::Status::OK();
}

arrow::Status mean_pass_num(
    const std::shared_ptr<arrow::Table>& table
) 
{
    /*
        Create a new column, trip_distance_group in which the values are short 
        (< 2 miles), medium ( 2 miles and 10 miles), and long (> 10 miles). 
        What is the average number of passengers per trip length category? Sort 
        this result from highest (most passengers) to lowest (fewest passengers)
    */
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    
    auto filter1 = 
        cp::call(
            "less_equal", {cp::field_ref("trip_distance"), cp::literal(2)});
    auto filter2 = 
            cp::call(
            "greater", {cp::field_ref("trip_distance"), cp::literal(10)});
    auto cond = 
        cp::call(
            "make_struct", 
            {std::move(filter1), std::move(filter2)}, 
            cp::MakeStructOptions({"a", "b"})
        );
    auto case_ = cp::call("case_when", 
        {
            cond, 
            cp::literal("short"), cp::literal("long"), cp::literal("medium")
        }
    ); 
    
    ac::Declaration project{
        "project", 
        {std::move(source)}, 
        ac::ProjectNodeOptions(
            {cp::field_ref("passenger_count"), case_}, 
            {"passenger_count","trip_distance_group"})
    };
    
    auto options = 
        std::make_shared<cp::ScalarAggregateOptions>(
            cp::ScalarAggregateOptions::Defaults());
    auto aggregate_options =
        ac::AggregateNodeOptions{
            {{"hash_mean", options, "passenger_count", "mean"}},
            {"trip_distance_group"}};
    ac::Declaration aggregate{
        "aggregate", {std::move(project)}, std::move(aggregate_options)};
    
    ac::OrderByNodeOptions opts{
        cp::Ordering{
            {cp::SortKey(arrow::FieldRef("mean"), cp::SortOrder::Descending)}
        }
    };
    ac::Declaration sort("order_by", {std::move(aggregate)}, std::move(opts));
    
    {
        timer t;
        ARROW_ASSIGN_OR_RAISE(auto new_table, 
            ac::DeclarationToTable(std::move(sort)));
        std::cout << new_table->ToString() << std::endl;   
    }
    return arrow::Status::OK();
}


arrow::Status concat_two_tables(std::shared_ptr<arrow::Table>& table)
{
    /*
        Create a single data frame containing rides from both January 2019 and 
        January 2020
    */
    {   
        timer t;
        std::vector<std::string> fields 
            = {"tpep_pickup_datetime", "passenger_count", "trip_distance", 
                "total_amount"};
        std::shared_ptr<arrow::Table> table_1, table_2;
        ARROW_RETURN_NOT_OK(read_file_to_table(
            "../data/nyc_taxi_2019-01.csv", table_1, fields));
        ARROW_RETURN_NOT_OK(read_file_to_table(
            "../data/nyc_taxi_2020-01.csv", table_2, fields));
        ARROW_ASSIGN_OR_RAISE(
            table, arrow::ConcatenateTables({table_1, table_2}));
    }
    return arrow::Status::OK();
}

arrow::Status mean_cost_by_year(const std::shared_ptr<arrow::Table>& table)
{
    /*
        with a column year indicating which year the ride comes 
        from use groupby to compare the average cost of a taxi in January from 
        each of two years
    */
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    cp::Expression year = 
        cp::call("year", {cp::field_ref("tpep_pickup_datetime")});

    ac::Declaration project{
        "project",
        {std::move(source)},
        ac::ProjectNodeOptions(
            {cp::field_ref("total_amount"), year}, {"total_amount", "year"})
    };
    
    auto options = 
        std::make_shared<cp::ScalarAggregateOptions>(
            cp::ScalarAggregateOptions::Defaults());
    auto aggregate_options =
        ac::AggregateNodeOptions{
            {{"hash_mean", options, "total_amount", "mean"}},
            {"year"}};
    ac::Declaration aggregate{
        "aggregate", {std::move(project)}, std::move(aggregate_options)};
    
    cp::Expression filter_ = cp::call("or", 
        {
            cp::call("equal", {cp::field_ref("year"), cp::literal(2019)}),
            cp::call("equal", {cp::field_ref("year"), cp::literal(2020)})
        });
    ac::Declaration filter{
        "filter",
        {std::move(aggregate)},
        ac::FilterNodeOptions(filter_)
    };
    
    
    {
        timer t;
        ARROW_ASSIGN_OR_RAISE(auto new_table, 
            ac::DeclarationToTable(std::move(filter)));
        std::cout << new_table->ToString() << std::endl;   
    }
    return arrow::Status::OK();
}


arrow::Status mean_cost_by_year_and_pass_count(
    const std::shared_ptr<arrow::Table>& table)
{
    /*
        Create a two-level grouping, first by year and then by passenger_count
    */
    ac::Declaration source{"table_source", ac::TableSourceNodeOptions{table}};
    cp::Expression year = 
        cp::call("year", {cp::field_ref("tpep_pickup_datetime")});
    cp::Expression coal = 
        cp::call("coalesce",{cp::field_ref("passenger_count"), cp::literal(0)});
    ac::Declaration project{
        "project",
        {std::move(source)},
        ac::ProjectNodeOptions(
            {
                cp::field_ref("total_amount"), 
                coal, year
            }, 
            {"total_amount",  "passenger_count", "year"}
        )
    };
    
    auto options = 
        std::make_shared<cp::ScalarAggregateOptions>(
            cp::ScalarAggregateOptions::Defaults());
    auto aggregate_options =
        ac::AggregateNodeOptions{
            {{"hash_mean", options, "total_amount", "mean"}},
            {"year", "passenger_count"}};
    ac::Declaration aggregate{
        "aggregate", {std::move(project)}, std::move(aggregate_options)};
    
    cp::Expression filter_ = cp::call("or", 
        {
            cp::call("equal", {cp::field_ref("year"), cp::literal(2019)}),
            cp::call("equal", {cp::field_ref("year"), cp::literal(2020)})
        });
    ac::Declaration filter{
        "filter",
        {std::move(aggregate)},
        ac::FilterNodeOptions(filter_)
    };
   
    ac::OrderByNodeOptions opts{
        cp::Ordering{
            {
                cp::SortKey(arrow::FieldRef("year"), cp::SortOrder::Ascending),
                cp::SortKey(arrow::FieldRef("passenger_count"), 
                    cp::SortOrder::Ascending)
            }
        }
    };
    ac::Declaration sort("order_by", {std::move(filter)}, std::move(opts));


    {
        timer t;
        ARROW_ASSIGN_OR_RAISE(auto new_table, 
            ac::DeclarationToTable(std::move(sort)));
        std::cout << new_table->ToString() << std::endl;   
    }
    return arrow::Status::OK();
}


void run_main_ch_6_2()
{
    arrow::Status st;
    std::shared_ptr<arrow::Table> table;
    st = read_file_to_table(
        "../data/nyc_taxi_2019-01.csv", 
        table, 
        { 
            "passenger_count", "trip_distance", "total_amount"
        });
    // st = mean_cost_taxi_ride(table, "mean", false); // 0.011925 s
    // st = mean_cost_taxi_ride(table, "passenger_count"); // 0.010796 s
    // st = mean_pass_num(table); // 0.035646 s
    st = concat_two_tables(table); // 0.383406 s
    // st = mean_cost_by_year(table); // 0.031645 s
    st = mean_cost_by_year_and_pass_count(table); // 0.035522 s
    std::cout << st.message() << "\n";
}

#endif