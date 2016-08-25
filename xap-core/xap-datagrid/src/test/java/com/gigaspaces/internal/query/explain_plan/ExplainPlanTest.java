package com.gigaspaces.internal.query.explain_plan;

import com.gigaspaces.internal.query.CompoundAndCustomQuery;
import com.gigaspaces.internal.query.CompoundOrCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.builder.range.ContainsCompositeRange;
import com.j_spaces.jdbc.builder.range.ContainsValueRange;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.FunctionCallDescription;
import com.j_spaces.jdbc.builder.range.SegmentRange;


import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
public class ExplainPlanTest {
    SpaceTypeDescriptor typeInfo = new SpaceTypeDescriptorBuilder(MyPojo.class, null).create();
    private static List<String> queries;




    @Test
    public void SimpleOrAndQueryTest() {
        TemplateEntryDataMock templateMock = createTemplateMock("(category > 5 OR category < 2) AND id > 50");
        ExplainPlan plan = new ExplainPlan(templateMock.getCustomQuery());
        String expected = "OR(\n" +
                "\tAND(\n" +
                "\t\tLT(category, 2)\n" +
                "\t\tGT(id, 50)\n" +
                "\t)\n" +
                "\tAND(\n" +
                "\t\tGT(category, 5)\n" +
                "\t\tGT(id, 50)\n" +
                "\t)\n" +
                ")";
       Assert.assertEquals(expected, plan.toString());
    }

    @Test
    public void SqlFunctionOrQueryTest() {
        TemplateEntryDataMock templateMock = createTemplateMock("MOD(category,3) > 1 OR MOD(category,3) = 0");
        ExplainPlan plan = new ExplainPlan(templateMock.getCustomQuery());
//        System.out.println(plan.toString());
        String expected = "OR(\n" +
                "\tEQ(MOD(category,3), 0)\n" +
                "\tGT(MOD(category,3), 1)\n" +
                ")";
        Assert.assertEquals(expected, plan.toString());

    }

    @Test
    public void ContainsCompositeQueryTest() {
        TemplateEntryDataMock templateMock = createTemplateMock("carsList[*].i <= 3 AND carsList[*].i >= 3");
        ExplainPlan plan = new ExplainPlan(templateMock.getCustomQuery());
//        System.out.println(plan.toString());
        String expected = "AND(\n" +
                "\tGE(carsList[*].i, 3)\n" +
                "\tLE(carsList[*].i, 3)\n" +
                ")";
        Assert.assertEquals(expected, plan.toString());

    }

    private TemplateEntryDataMock createTemplateMock(String s) {
        ICustomQuery customQuery;
        Object[] _fieldsValues;
        short[] _extendedMatchCodes;
        int i = queries.indexOf(s);
        switch (i) {
            case 0: //"(category > 5 OR category < 2) AND id > 50"
                List<ICustomQuery> andList = new ArrayList<ICustomQuery>();
                andList.add(new SegmentRange("category", null, false, 2, false));
                andList.add(new SegmentRange("id", 50, false, null, false));
                CompoundAndCustomQuery andCustomQuery = new CompoundAndCustomQuery(andList);
                List<ICustomQuery> andList2 = new ArrayList<ICustomQuery>();
                andList2.add(new SegmentRange("category", 5, false,null , false));
                andList2.add(new SegmentRange("id", 50, false, null, false));
                CompoundAndCustomQuery andCustomQuery1 = new CompoundAndCustomQuery(andList2);
                List<ICustomQuery> orList = new ArrayList<ICustomQuery>();
                orList.add(andCustomQuery);
                orList.add(andCustomQuery1);
                customQuery = new CompoundOrCustomQuery(orList);
                return new TemplateEntryDataMock(customQuery,null, null);
            case 1: //"MOD(category,3) > 1 OR MOD(category,3) = 0"
                List<ICustomQuery> orList2 = new ArrayList<ICustomQuery>();
                List<Object> args = new ArrayList<Object>();
                args.add(3);
                FunctionCallDescription mod = new FunctionCallDescription("MOD", 1, args);
                orList2.add(new EqualValueRange("category", mod, 0));
                orList2.add(new SegmentRange("category", mod, 1, false, null, false));
                customQuery = new CompoundOrCustomQuery(orList2);
                return new TemplateEntryDataMock(customQuery,null, null);
            case 2: //"carsList[*].i <= 3 AND carsList[*].i >= 3"
                ContainsCompositeRange containsCompositeRange = new ContainsCompositeRange(new ContainsValueRange("carsList[*].i", null, 3, (short) 3), new ContainsValueRange("carsList[*].i", null, 3, (short) 5));
                List<ICustomQuery> andList3 = new ArrayList<ICustomQuery>();
                andList3.add(containsCompositeRange);
                customQuery = new CompoundAndCustomQuery(andList3);
                return new TemplateEntryDataMock(customQuery, null, null);
            default: return null;
        }
    }

    @BeforeClass
    public static void initQueryList(){
        queries= new ArrayList<String>();
        queries.add(0,"(category > 5 OR category < 2) AND id > 50");
        queries.add(1,"MOD(category,3) > 1 OR MOD(category,3) = 0");
        queries.add(2,"carsList[*].i <= 3 AND carsList[*].i >= 3");

//        queries.add(2,"(carsList[*].color = ‘red’) AND id > 50\n");
//        queries.add(3,"(carsList[*].company = 'Honda') AND (carsList[*].color = 'red')");
//        queries.add(4,"carsList[*](company = 'Honda') AND (carsList[*].color = 'red')");
//        queries.add(5,"carsList[*](company = 'Honda' AND color = 'red')");
//        queries.add(6,"carsList[*](company = 'Honda') OR carsList[*](color = 'red')");
//        queries.add(7,"location spatial:within circle(0,0,3)");
//        queries.add(8,"location spatial:within circle(0,0,3) AND spatial:within circle(1,2,3)");
//        queries.add(9,"carsList[*].color = ‘red’");
    }































}