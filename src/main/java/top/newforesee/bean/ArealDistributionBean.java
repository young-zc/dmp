package top.newforesee.bean;

/**
 * xxx
 * creat by newforesee 2019/1/21
 */
public class ArealDistributionBean {
    /**
     * 原始请求
     */
    private int originalRequest;
    /**
     * 有效请求
     */
    private int validRequest;
    /**
     * 广告请求
     */
    private int advRequest;
    /**
     * 参与竞价数
     */
    private int partInBidding;
    /**
     * 竞价成功数
     */
    private int successBidding;
    /**
     * 竞价成功率
     */
    private double biddingRate;
    /**
     * 展示量
     */
    private int show;
    /**
     * 点击量
     */
    private int click;
    /**
     * 点击率
     */
    private double clickRate;
    /**
     * 广告成本
     */
    private double advCost;
    /**
     * 广告消费
     */
    private double advCharge;



    public int getOriginalRequest() {
        return originalRequest;
    }

    public void setOriginalRequest(int originalRequest) {
        this.originalRequest = originalRequest;
    }

    public int getValidRequest() {
        return validRequest;
    }

    public void setValidRequest(int validRequest) {
        this.validRequest = validRequest;
    }

    public int getAdvRequest() {
        return advRequest;
    }

    public void setAdvRequest(int advRequest) {
        this.advRequest = advRequest;
    }

    public int getPartInBidding() {
        return partInBidding;
    }

    public void setPartInBidding(int partInBidding) {
        this.partInBidding = partInBidding;
    }

    public int getSuccessBidding() {
        return successBidding;
    }

    public void setSuccessBidding(int successBidding) {
        this.successBidding = successBidding;
    }

    public double getBiddingRate() {
        return biddingRate;
    }

    public void setBiddingRate(double biddingRate) {
        this.biddingRate = biddingRate;
    }

    public int getShow() {
        return show;
    }

    public void setShow(int show) {
        this.show = show;
    }

    public int getClick() {
        return click;
    }

    public void setClick(int click) {
        this.click = click;
    }

    public double getClickRate() {
        return clickRate;
    }

    public void setClickRate(double clickRate) {
        this.clickRate = clickRate;
    }

    public double getAdvCost() {
        return advCost;
    }

    public void setAdvCost(double advCost) {
        this.advCost = advCost;
    }

    public double getAdvCharge() {
        return advCharge;
    }

    public void setAdvCharge(double advCharge) {
        this.advCharge = advCharge;
    }

    @Override
    public String toString() {
        return "{" +
                "originalRequest=" + originalRequest +
                ", validRequest=" + validRequest +
                ", advRequest=" + advRequest +
                ", partInBidding=" + partInBidding +
                ", successBidding=" + successBidding +
                ", biddingRate=" + biddingRate +
                ", show=" + show +
                ", click=" + click +
                ", clickRate=" + clickRate +
                ", advCost=" + advCost +
                ", advCharge=" + advCharge +
                '}';
    }
}
