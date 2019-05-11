package top.newforesee.dao.impl;

import org.apache.commons.dbutils.QueryRunner;
import top.newforesee.bean.CityCountBean;
import top.newforesee.dao.ICityCount;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;
import java.util.List;

/**
 * ICityCount的实现类,对数据库进行操作
 * creat by newforesee 2019/1/21
 */
public class CityCountImpl implements ICityCount {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());
    @Override
    public void saveToDB(List<CityCountBean> beans) {
        String sql = "insert into city_count values(?,?,?)";
        Object[][] objs = new Object[beans.size()][];
        for (int i = 0; i < objs.length; i++) {
            CityCountBean bean = beans.get(i);
            objs[i]=new Object[]{
                    bean.getCt(),
                    bean.getProvinceName(),
                    bean.getCityName()
            };
        }
        try {
            qr.batch(sql,objs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
