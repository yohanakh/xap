package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

import java.awt.*;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */

public class MyPojo {

    private Integer id;
    private String content;
    private Integer category;
    private List<Car> carsList;
    private Point location;

    public MyPojo() {
    }

    public MyPojo(Integer id, String content, Integer category, Point location) {
        this.id = id;
        this.content = content;
        this.category = category;
        this.location = location;

    }

    @SpaceIndex(type = SpaceIndexType.BASIC)
    @SpaceId(autoGenerate = false)
    public Integer getId() {
        return id;
    }


    public void setId(Integer id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Integer getcategory() {
        return category;
    }

    public void setcategory(Integer category) {
        this.category = category;
    }

    public List<Car> getCarsList() {
        return carsList;
    }

    public void setCarsList(List<Car> carsList) {
        this.carsList = carsList;
    }

    public Point getLocation() {
        return location;
    }


    public void setLocation(Point location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "MyPojo{" +
                "id=" + id +
                ", content='" + content + '\'' +
                ", category=" + category +
                ", carsList=" + carsList +
                ", location=" + location +
                '}';
    }

    private class Car {
        private String company;
        private String color;
        private String id;


        public Car() {
        }

        public Car(String company, String color, String id) {
            this.company = company;
            this.color = color;
            this.id = id;
        }

        public String getCompany() {
            return company;
        }

        public String getId() {
            return id;
        }

        @SpaceId
        public void setId(String id) {
            this.id = id;
        }

        public String getColor() {
            return color;
        }

        public void setCompany(String company) {
            this.company = company;
        }

        public void setColor(String color) {
            this.color = color;
        }

        @Override
        public String toString() {
            return "Car{" +
                    "company='" + company + '\'' +
                    ", color='" + color + '\'' +
                    ", id='" + id + '\'' +
                    '}';
        }
    }
}