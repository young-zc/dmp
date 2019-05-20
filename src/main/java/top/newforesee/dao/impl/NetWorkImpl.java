package top.newforesee.dao.impl;

import org.apache.commons.dbutils.QueryRunner;
import top.newforesee.bean.NetworkType;
import top.newforesee.dao.INetworkImpl;
import top.newforesee.utils.DBCPUtil;

import java.sql.SQLException;
import java.util.List;

/**
 * xxx
 * creat by newforesee 2019-01-26
 */
public class NetWorkImpl implements INetworkImpl {
    private QueryRunner qr = new QueryRunner(DBCPUtil.getDataSource());
    @Override
    public void saveToDB(List<NetworkType> beans) {

        String sql = "insert into network_types values(?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[][] objs = new Object[beans.size()][];
        for (int i = 0; i < objs.length; i++) {
            NetworkType bean =  beans.get(i);
            objs[i]=new Object[]{
                bean.getNetworkTypes(),
                    bean.getOriginalRequest(),
                    bean.getValidRequest(),
                    bean.getAdvRequest(),
                    bean.getPartInBidding(),
                    bean.getSuccessBidding(),
                    bean.getShow(),
                    bean.getClick(),
                    bean.getAdvCost(),
                    bean.getAdvCharge(),
                    bean.getBiddingRate(),
                    bean.getClickRate()

            };
        }
        try {
            qr.batch(sql,objs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
