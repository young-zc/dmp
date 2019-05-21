package top.newforesee.bean;

/**
 * xxx
 * creat by newforesee 2019-01-26
 */
public class NetworkType {

    private String networkTypes;
    private int originalRequest;
    private int validRequest;
    private int advRequest;
    private int partInBidding;
    private int successBidding;
    private int show;
    private int click;
    private int advCost;
    private int advCharge;
    private double biddingRate;
    private double clickRate;

    public NetworkType() {
    }

    public NetworkType(String networkTypes, int originalRequest, int validRequest, int advRequest, int partInBidding, int successBidding, int show, int click, int advCost, int advCharge, double biddingRate, double clickRate) {
        this.networkTypes = networkTypes;
        this.originalRequest = originalRequest;
        this.validRequest = validRequest;
        this.advRequest = advRequest;
        this.partInBidding = partInBidding;
        this.successBidding = successBidding;
        this.show = show;
        this.click = click;
        this.advCost = advCost;
        this.advCharge = advCharge;
        this.biddingRate = biddingRate;
        this.clickRate = clickRate;
    }

    public String getNetworkTypes() {
        return networkTypes;
    }

    public void setNetworkTypes(String networkTypes) {
        this.networkTypes = networkTypes;
    }

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

    public int getAdvCost() {
        return advCost;
    }

    public void setAdvCost(int advCost) {
        this.advCost = advCost;
    }

    public int getAdvCharge() {
        return advCharge;
    }

    public void setAdvCharge(int advCharge) {
        this.advCharge = advCharge;
    }

    public double getBiddingRate() {
        return biddingRate;
    }

    public void setBiddingRate(double biddingRate) {
        this.biddingRate = biddingRate;
    }

    public double getClickRate() {
        return clickRate;
    }

    public void setClickRate(double clickRate) {
        this.clickRate = clickRate;
    }

    @Override
    public String toString() {
        return "{" +
                "networkTypes='" + networkTypes + '\'' +
                ", originalRequest=" + originalRequest +
                ", validRequest=" + validRequest +
                ", advRequest=" + advRequest +
                ", partInBidding=" + partInBidding +
                ", successBidding=" + successBidding +
                ", show=" + show +
                ", click=" + click +
                ", advCost=" + advCost +
                ", advCharge=" + advCharge +
                ", biddingRate=" + biddingRate +
                ", clickRate=" + clickRate +
                '}';
    }
}
