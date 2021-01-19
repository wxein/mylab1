package com.dbs.celerity.queryrunner.api.filequery;

import com.dbs.celerity.queryrunner.pojo.Tree;
import com.netflix.config.DynamicStringProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RestController;
import org.thymeleaf.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController

public class FileQueryController {
    List <Tree>node=new LinkedList();
    private static final Logger LOGGER = LoggerFactory.getLogger(FileQueryController.class);
    private static final DynamicStringProperty DIRECTORY_PATH = new DynamicStringProperty("filequery.directorypath.url", null);

    @RequestMapping("file")
    public List<Tree> file() throws UnsupportedEncodingException {
        LOGGER.info("Started File Query Process");
        node.removeAll(node);
        //String path=DIRECTORY_PATH.get();
        String path="./";
        int level=0;
        List<Tree>file=getFile(path,1,level);
        LOGGER.info("Contents for file query" + file);
        return file;
    }

    private  List<Tree> getFile(String path,int id,int pid) throws UnsupportedEncodingException {
        File file = new File(path);
        if(file.exists()) {
            File[] array = file.listFiles();

            List fileList = Arrays.asList(array);

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

                //check whether it is directory
                if (array[i].isDirectory()) {
                    node.add(tree);
                    getFile(array[i].getPath(), id * 10 + 1 + i, id);
                    id++;
                } else {
                    tree.setUrl("/file/download?filePath="+java.net.URLEncoder.encode(array[i].getAbsolutePath(),"utf-8").replace("\\","/"));
                    node.add(tree);
                    id++;
                }
            }
        } else {
            System.out.println("File not exist");
        }
        return node;
    }

    @RequestMapping("file/download")
    public String download(HttpServletRequest request, HttpServletResponse response, String filePath){
        File fileurl = new File(filePath);

        String showValue =filePath.substring(filePath.lastIndexOf("/")+1);;
        System.out.println(showValue);
        try{
            InputStream inStream = new FileInputStream(fileurl);

            final String userAgent = request.getHeader("USER-AGENT");

            String finalFileName = null;
            if(StringUtils.contains(userAgent, "MSIE")||StringUtils.contains(userAgent,"Trident")){
                finalFileName = URLEncoder.encode(showValue,"UTF8");

            }else if(StringUtils.contains(userAgent, "Mozilla")){
                finalFileName = new String(showValue.getBytes(), "ISO8859-1");
            }else{
                finalFileName = URLEncoder.encode(showValue,"UTF8");
            }

            response.reset();
            response.setContentType("application/x-download");
            response.addHeader("Content-Disposition" ,"attachment;filename=\"" +finalFileName+ "\"");


            byte[] b = new byte[1024];
            int len;
            while ((len = inStream.read(b)) > 0){
                response.getOutputStream().write(b, 0, len);
            }
            inStream.close();
            response.getOutputStream().close();
        }catch(Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}
