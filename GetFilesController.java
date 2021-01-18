package com.example.demo.controller;

import com.example.demo.pojo.Tree;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.*;

import org.springframework.web.bind.annotation.CrossOrigin;

@CrossOrigin(origins = "*", allowedHeaders = "*")
@Controller
public class GetFilesController {
    List <Tree>node=new LinkedList();
    @ResponseBody
    @RequestMapping("/file")
    public List<Tree> file() throws UnsupportedEncodingException {
        node.removeAll(node);
        String path="D:\\SourceCode\\Python Qt GUI与数据可视化编程";
        int level=0;
        List<Tree>file=getFile(path,1,level);
        return file;
    }
    private  List<Tree> getFile(String path,int id,int pid) throws UnsupportedEncodingException {
        File file = new File(path);
        if(file.exists()) {
            File[] array = file.listFiles();

            List fileList = Arrays.asList(array);
            //对读到的本地文件夹进行排序
            Collections.sort(fileList, new Comparator<File>() {
                @Override
                public int compare(File o1, File o2) {
                    if (o1.isDirectory() && o2.isFile())
                        return -1;
                    if (o1.isFile() && o2.isDirectory())
                        return 1;
                    return o1.getName().compareTo(o2.getName());
                }
            });

            for (int i = 0; i < array.length; i++) {
                Tree tree = new Tree();
                tree.setpId(pid);
                tree.setId(id);
                tree.setName(array[i].getName());
                tree.setUrl("http://localhost:8080/download?filePath="+java.net.URLEncoder.encode(array[i].getAbsolutePath(),"utf-8").replace("\\","/"));
                //判断是否为文件夹，是的话进行递归
                if (array[i].isDirectory()) {
                    node.add(tree);
                    //进行递归，此时的pid为上一级的id
                    getFile(array[i].getPath(), id * 10 + 1 + i, id);
                    id++;
                } else {
                    node.add(tree);
                    id++;
                }
            }
        } else {
            System.out.println("文件不存在");
        }
        return node;
    }
}

