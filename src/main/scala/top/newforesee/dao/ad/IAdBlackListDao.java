package top.newforesee.dao.ad;


import top.newforesee.bean.ad.AdBlackList;

import java.util.List;

/**
 * Description：对某个广告点击超过100次的黑名单用户操作dao层接口<br/>
 */
public interface IAdBlackListDao {

    /**
     * 查询所有黑名单信息
     *
     * @return
     */
    List<AdBlackList> findAllAdBlackList();

    /**
     * 批量更新黑名单（保存）
     */
    void updateBatch(List<AdBlackList> beans);
}
