package spark300.class39;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * @Author: Liufeifan
 * @Date: 2020/7/22 15:36
 */
public class SecondarySortKey implements Ordered<SecondarySortKey> , Serializable {

    private static final long serialVersionUID = 4247136040666717354L;
    //定义需要排序的信息
    private int first;
    private  int second;

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compare( SecondarySortKey that) {
        if(this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        }else {
            return this.second - that.getSecond();
        }
    }
    @Override
    public int compareTo(SecondarySortKey that) {
        if(this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        }else {
            return this.second - that.getSecond();
        }
    }
    @Override
    public boolean $less( SecondarySortKey that) {
        if(this.first < that.getFirst()){
            return true;
        }else if(this.first == that.getFirst() &&
                this.second < that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKey that) {
        if(this.first > that.getFirst()){
            return  true;
        } else if (this.first == that.getFirst() &&
                this.second > that.getSecond()){
            return false;
        }
        return false;
    }

    @Override
    public boolean $less$eq( SecondarySortKey that) {
        if (this.$less(that)){
            return true;
        }else if(this.first == that.getFirst() &&
                this.second == that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq( SecondarySortKey that) {
        if(this.$greater(that)){
            return true;
        }
        else if(this.first == that.getFirst() &&
                this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    //为要进行排序的多个列，提供getter 和setter方法


    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
