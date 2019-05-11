package top.newforesee.dao;


import top.newforesee.bean.CityCountBean;

import java.util.List;

/**
 * xxx
 * creat by newforesee 2019/1/21
 */
public interface ICityCount {
   void saveToDB(List<CityCountBean> beans);
}
