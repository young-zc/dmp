package top.newforesee.bean;

import scala.Serializable;

/**
 * xxx
 * creat by newforesee 2019/1/21
 */
public class CityCountBean  implements Serializable{
    private int ct;
    private String provinceName ;
    private String cityName;

    public int getCt() {
        return ct;
    }

    public void setCt(int ct) {
        this.ct = ct;
    }

    public String getProvinceName() {
        return provinceName;
    }

    public void setProvinceName(String provinceName) {
        this.provinceName = provinceName;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    @Override
    public String toString() {
        return "{" +
                "ct=" + ct +
                ", provinceName='" + provinceName + '\'' +
                ", cityName='" + cityName + '\'' +
                '}';
    }
}
