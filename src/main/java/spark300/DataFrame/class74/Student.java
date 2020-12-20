package spark300.DataFrame.class74;

import java.io.Serializable;

/**
 * @Author: Liufeifan
 * @Date: 2020/9/10 16:52
 */
public  class Student implements Serializable {
    private int id;
    private String name;
    private  int age;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
