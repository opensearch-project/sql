/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.ppl;

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute$;
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar$;
import org.apache.spark.sql.catalyst.analysis.UnresolvedTable;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstExpressionBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;
import org.opensearch.sql.spark.client.SparkClient;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.internal.Trees;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.List.of;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static scala.collection.JavaConverters.asScalaBuffer;


public class PPLToCatalystLogicalPlanTranslatorTest {
    private PPLSyntaxParser parser = new PPLSyntaxParser();
    @Mock
    private SparkClient sparkClient;

    @Mock
    private LogicalProject logicalProject;
    private CatalystPlanContext context = new CatalystPlanContext();

    private Statement plan(String query, boolean isExplain) {
        final AstStatementBuilder builder =
                new AstStatementBuilder(
                        new AstBuilder(new AstExpressionBuilder(), query),
                        AstStatementBuilder.StatementBuilderContext.builder().isExplain(isExplain).build());
        return builder.visit(parser.parse(query));
    }

    @Test
    /**
     * test simple search with only one table and no explicit fields (defaults to all fields)
     */
    void testSearchWithTableAllFieldsPlan() {
        Statement plan = plan("search source = table ", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        Seq<?> projectList = JavaConverters.asScalaBuffer(Collections.singletonList((Object) UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()))).toSeq();
        Project expectedPlan = new Project((Seq<NamedExpression>) projectList, new UnresolvedTable(asScalaBuffer(of("table")).toSeq(), "source=table ", Option.<String>empty()));
        Assertions.assertEquals(context.getPlan().toString(), expectedPlan.toString());
    }

    @Test
    /**
     * test simple search with only one table and no explicit fields (defaults to all fields)
     */
    void testSourceWithTableAllFieldsPlan() {
        Statement plan = plan("source = table ", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        Seq<?> projectList = JavaConverters.asScalaBuffer(Collections.singletonList((Object) UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()))).toSeq();
        Project expectedPlan = new Project((Seq<NamedExpression>) projectList, new UnresolvedTable(asScalaBuffer(of("table")).toSeq(), "source=table ", Option.<String>empty()));
        Assertions.assertEquals(context.getPlan().toString(), expectedPlan.toString());
    }

    @Test
    /**
     * test simple search with only one table with one field projected
     */
    void testSourceWithTableOneFieldPlan() {
        Statement plan = plan("source=table | fields A", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        // Create a Project node for fields A and B
        List<NamedExpression> projectList = Arrays.asList(
                UnresolvedAttribute$.MODULE$.apply(JavaConverters.asScalaBuffer(Collections.singletonList("A")))
        );
        Project expectedPlan = new Project(JavaConverters.asScalaBuffer(projectList).toSeq(), new UnresolvedTable(asScalaBuffer(of("table")).toSeq(), "source=table ", Option.<String>empty()));
        Assertions.assertEquals(context.getPlan().toString(), expectedPlan.toString());
    }

    @Test
    /**
     * test simple search with only one table with one field literal filtered 
     */
    void testSourceWithTableAndConditionPlan() {
        Statement plan = plan("source=t a = 1 ", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        // Create a Project node for fields A and B
        List<NamedExpression> projectList = Arrays.asList(
                UnresolvedAttribute$.MODULE$.apply(JavaConverters.asScalaBuffer(Collections.singletonList("a")))
        );
        UnresolvedTable table = new UnresolvedTable(asScalaBuffer(of("table")).toSeq(), "source=table ", Option.<String>empty());
        // Create a Filter node for the condition 'a = 1'
        EqualTo filterCondition = new EqualTo((Expression) projectList.get(0), Literal.create(1, IntegerType));
        LogicalPlan filterPlan = new Filter(filterCondition, table);
        Assertions.assertEquals(context.getPlan().toString(), filterPlan.toString());
    }

    @Test
    /**
     * test simple search with only one table with two fields projected
     */
    void testSourceWithTableTwoFieldPlan() {
        Statement plan = plan("source=table | fields A, B", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        // Create a Project node for fields A and B
        List<NamedExpression> projectList = Arrays.asList(
                UnresolvedAttribute$.MODULE$.apply(JavaConverters.asScalaBuffer(Collections.singletonList("A"))),
                UnresolvedAttribute$.MODULE$.apply(JavaConverters.asScalaBuffer(Collections.singletonList("B")))
        );
        Project expectedPlan = new Project(JavaConverters.asScalaBuffer(projectList).toSeq(), new UnresolvedTable(asScalaBuffer(of("table")).toSeq(), "source=table ", Option.<String>empty()));
        Assertions.assertEquals(context.getPlan().toString(), expectedPlan.toString());
    }

    @Test
    /**
     * Search multiple tables - translated into union call
     */
    void testSearchWithMultiTablesPlan() {
        Statement plan = plan("search source = table1, table2 ", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        Seq<?> projectList = JavaConverters.asScalaBuffer(Collections.singletonList((Object) UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()))).toSeq();
        Project expectedPlanTable1 = new Project((Seq<NamedExpression>) projectList, new UnresolvedTable(asScalaBuffer(of("table1")).toSeq(), "source=table ", Option.<String>empty()));
        Project expectedPlanTable2 = new Project((Seq<NamedExpression>) projectList, new UnresolvedTable(asScalaBuffer(of("table2")).toSeq(), "source=table ", Option.<String>empty()));
        // Create a Union logical plan
        Seq<LogicalPlan> unionChildren = JavaConverters.asScalaBuffer(Arrays.asList((LogicalPlan) expectedPlanTable1, (LogicalPlan) expectedPlanTable2)).toSeq();
        // todo : parameterize the union options byName & allowMissingCol
        LogicalPlan unionPlan = new Union(unionChildren, true, false);
        Assertions.assertEquals(context.getPlan().toString(), unionPlan.toString());
    }

    @Test
    /**
     * Find What are the average prices for different types of properties?
     */
    void testFindAvgPricesForVariousPropertyTypes() {
        Statement plan = plan("source = housing_properties | stats avg(price) by property_type", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }

   @Test
    /**
     * Find the top 10 most expensive properties in California, including their addresses, prices, and cities
     */
    void testFindTopTenExpensivePropertiesCalifornia() {
        Statement plan = plan("source = housing_properties | where state = \"CA\" | fields address, price, city | sort - price | head 10", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }

    @Test
    /**
     * Find the average price per unit of land space for properties in different cities
     */
    void testFindAvgPricePerUnitByCity() {
        Statement plan = plan("source = housing_properties | where land_space > 0 | eval price_per_land_unit = price / land_space | stats avg(price_per_land_unit) by city", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }

    @Test
    /**
     * Find the houses posted in the last month, how many are still for sale
     */
    void testFindHousesForSaleDuringLastMonth() {
        Statement plan = plan("search source=housing_properties | where listing_age >= 0 | where listing_age < 30 | stats count() by property_status", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }

    @Test
    /**
     * Find all the houses listed by agency Compass in  decreasing price order. Also provide only price, address and agency name information.
     */
    void testFindHousesByDecreasePriceWithSpecificFields() {
        Statement plan = plan("source = housing_properties | where match( agency_name , \"Compass\" ) | fields address , agency_name , price | sort - price ", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }    
    @Test
    /**
     * Find details of properties owned by Zillow with at least 3 bedrooms and 2 bathrooms
     */
    void testFindHousesByOwnedByZillowWithMinimumOfRoomsWithSpecificFields() {
        Statement plan = plan("source = housing_properties | where is_owned_by_zillow = 1 and bedroom_number >= 3 and bathroom_number >= 2 | fields address, price, city, listing_age", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }
    @Test
    /**
     * Find which cities in WA state have the largest number of houses for sale?
     */
    void testFindCitiesInWAHavingLargeNumbrOfHouseForSale() {
        Statement plan = plan("source = housing_properties | where property_status = 'FOR_SALE' and state = 'WA' | stats count() as count by city | sort -count | head", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }
    @Test
    /**
     * Find the top 5 referrers for the "/" path in apache access logs?
     */
    void testFindTopFiveReferrers() {
        Statement plan = plan("source = access_logs | where path = \"/\" | top 5 referer", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }
    @Test
    /**
     * Find access paths by status code. How many error responses (status code 400 or higher) are there for each access path in the Apache access logs?
     */
    void testFindCountAccessLogsByStatusCode() {
        Statement plan = plan("source = access_logs | where status >= 400 | stats count() by path, status", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }  
    @Test
    /**
     * Find max size of nginx access requests for every 15min.
     */
    void testFindMaxSizeOfNginxRequestsWithWindowTimeSpan() {
        Statement plan = plan("source = access_logs | stats max(size)  by span( request_time , 15m) ", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }
    @Test
    /**
     * Find nginx logs with non 2xx status code and url containing 'products'
     */
    void testFindNginxLogsWithNon2XXStatusAndProductURL() {
        Statement plan = plan("source = sso_logs-nginx-* | where match(http.url, 'products') and http.response.status_code >= \"300\"", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }   
    @Test
    /**
     * Find What are the details (URL, response status code, timestamp, source address) of events in the nginx logs where the response status code is 400 or higher?
     */
    void testFindDetailsOfNginxLogsWithResponseAbove400() {
        Statement plan = plan("source = sso_logs-nginx-* | where http.response.status_code >= \"400\" | fields http.url, http.response.status_code, @timestamp, communication.source.address", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }
    @Test
    /**
     * Find What are the average and max http response sizes, grouped by request method, for access events in the nginx logs?
     */
    void testFindAvgAndMaxHttpResponseSizeGroupedBy() {
        Statement plan = plan("source = sso_logs-nginx-* | where event.name = \"access\" | stats avg(http.response.bytes), max(http.response.bytes) by http.request.method", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }    
    @Test
    /**
     * Find flights from which carrier has the longest average delay for flights over 6k miles?
     */
    void testFindFlightsWithCarrierHasLongestAvgDelayWithLongFlights() {
        Statement plan = plan("source = opensearch_dashboards_sample_data_flights | where DistanceMiles > 6000 | stats avg(FlightDelayMin) by Carrier | sort -`avg(FlightDelayMin)` | head 1", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }
    
    @Test
    /**
     * Find What's the average ram usage of windows machines over time aggregated by 1 week? 
     */
    void testFindAvgRamUsageOfWindowsMachineOverTime() {
        Statement plan = plan("source = opensearch_dashboards_sample_data_logs | where match(machine.os, 'win') | stats avg(machine.ram) by span(timestamp,1w)", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        //todo add expected catalyst logical plan & compare
        Assertions.assertEquals(false,true);
    }

}


