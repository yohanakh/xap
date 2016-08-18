package com.gigaspaces.internal.query.explain_plan;

import com.gigaspaces.internal.query.CompoundAndCustomQuery;
import com.gigaspaces.internal.query.CompoundOrCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.j_spaces.jdbc.builder.range.ContainsValueRange;
import com.j_spaces.jdbc.builder.range.SegmentRange;

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
    private List<String> queries = new ArrayList<String>();




    @Test
    public void SimpleOrAndQueryTest() {
        TemplateEntryDataMock templateMock = createTemplateMock("(catagory > 5 OR catagory < 2) AND id > 50");
        ExplainPlan plan = new ExplainPlan(templateMock);
    }

    private TemplateEntryDataMock createTemplateMock(String s) {
        ICustomQuery _customQuery;
        Object[] _fieldsValues;
        short[] _extendedMatchCodes;
        int i = queries.indexOf(s);
        switch (i) {
            case 1: //"(catagory > 5 OR catagory < 2) AND id > 50"
                List<ICustomQuery> andList = new ArrayList<ICustomQuery>();
                andList.add(new SegmentRange("catagory", null, false, 2, false));
                andList.add(new SegmentRange("id", 50, false, null, false));
                CompoundAndCustomQuery andCustomQuery = new CompoundAndCustomQuery(andList);
                List<ICustomQuery> andList2 = new ArrayList<ICustomQuery>();
                andList2.add(new SegmentRange("catagory", null, false, 5, false));
                andList2.add(new SegmentRange("id", 50, false, null, false));
                CompoundAndCustomQuery andCustomQuery1 = new CompoundAndCustomQuery(andList2);
                List<ICustomQuery> orList = new ArrayList<ICustomQuery>();
                orList.add(andCustomQuery);
                orList.add(andCustomQuery1);
                _customQuery = new CompoundOrCustomQuery(orList);
                return new TemplateEntryDataMock(_customQuery,null, null);
            default: return null;
        }
    }

    @BeforeClass
    public void initQueryList(){
        queries.add(1,"(catagory > 5 OR catagory < 2) AND id > 50");
        queries.add(2,"carsList[*].color = ‘red’");
        queries.add(3,"(carsList[*].color = ‘red’) AND id > 50\n");
        queries.add(4,"(carsList[*].company = 'Honda') AND (carsList[*].color = 'red')");
        queries.add(5,"carsList[*](company = 'Honda') AND (carsList[*].color = 'red')");
        queries.add(6,"carsList[*](company = 'Honda' AND color = 'red')");
        queries.add(7,"carsList[*](company = 'Honda') OR carsList[*](color = 'red')");
        queries.add(8,"location spatial:within circle(0,0,3)");
        queries.add(9,"location spatial:within circle(0,0,3) AND spatial:within circle(1,2,3)");
        queries.add(10,"MOD(catagory,3) > 1 OR MOD(catagory,3) = 0");
    }































}