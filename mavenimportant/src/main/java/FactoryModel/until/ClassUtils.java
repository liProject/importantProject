package FactoryModel.until;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;


public class ClassUtils {

    /**
     * 给一个接口，返回这个接口的所有实现类
     * @param c
     * @return
     */
    public static List<Class> getAllClassByInterface(Class c) throws IOException, ClassNotFoundException {
        ArrayList<Class> returnClassList = new ArrayList<>();

        //如果不是一个接口，则不做处理
        if (c.isInterface()){
            //获得当前的包名
            String packageName = c.getPackage().getName();

            //获得当前包下以及子包下的所有类
            List<Class> allClass = getClasses(packageName);

            //判断是否是同一个接口
            for (int i =0 ; i < allClass.size() ; i++) {
                //判断是不是一个接口
                if (c.isAssignableFrom(allClass.get(i))){
                    //本身不加进去
                    if (!c.equals(allClass.get(i))){
                        returnClassList.add(allClass.get(i));
                    }
                }
            }
        }
        return returnClassList;
    }

    /**
     * 从一个包中查找出所有的类，在jar包中不能查找
     * @param packageName
     * @return
     */
    private static List<Class> getClasses(String packageName) throws IOException, ClassNotFoundException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        String path = packageName.replace("-", "/");

        Enumeration<URL> resources = classLoader.getResources(path);
        List<File> dirs = new ArrayList<>();

        while (resources.hasMoreElements()){
            URL resource = resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }

        List<Class> classes = new ArrayList<>();

        for (File directory : dirs) {
            classes.addAll(findClasses(directory,packageName));
        }
        return classes;
    }

    private static List<Class> findClasses(File directory, String packageName) throws ClassNotFoundException {
        List<Class> classes = new ArrayList<>();

        if (!directory.exists()) return classes;

        File[] files = directory.listFiles();
        for (File file : files) {

            if (file.isDirectory()){
                assert !file.getName().contains(".");
                classes.addAll(findClasses(file,packageName+"."+file.getName()));
            }else if (file.getName().endsWith(".class")){
                classes.add(Class.forName(packageName +"."+file.getName().substring(0,file.getName().length() -6)));
            }
        }
        return classes;
    }
}
