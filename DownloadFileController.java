package com.example.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.thymeleaf.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URLEncoder;

@Controller
public class DownloadFileController {
//    @GetMapping("/info")
////    @ResponseBody
//    public String getInfoFromIdea() {
//        return "tree2";
//    }

    /**
     * 下载文件
     *
     *
     * @return
     * @throws Exception
     */
    @ResponseBody
    @RequestMapping(value= "download")
    public String download(HttpServletRequest request, HttpServletResponse response, String filePath){
        File fileurl = new File(filePath);
        //浏览器下载后的文件名称showValue,从url中截取到源文件名称以及，以及文件类型，如board.docx;
        String showValue =filePath.substring(filePath.lastIndexOf("/")+1);;
        System.out.println(showValue);
        try{
            //根据条件得到文件路径

            //将文件读入文件流
            InputStream inStream = new FileInputStream(fileurl);
            //获得浏览器代理信息
            final String userAgent = request.getHeader("USER-AGENT");
            //判断浏览器代理并分别设置响应给浏览器的编码格式
            String finalFileName = null;
            if(StringUtils.contains(userAgent, "MSIE")||StringUtils.contains(userAgent,"Trident")){//IE浏览器
                finalFileName = URLEncoder.encode(showValue,"UTF8");

            }else if(StringUtils.contains(userAgent, "Mozilla")){//google,火狐浏览器
                finalFileName = new String(showValue.getBytes(), "ISO8859-1");
            }else{
                finalFileName = URLEncoder.encode(showValue,"UTF8");//其他浏览器
            }
            //设置HTTP响应头
            response.reset();//重置 响应头
            response.setContentType("application/x-download");//告知浏览器下载文件，而不是直接打开，浏览器默认为打开
            response.addHeader("Content-Disposition" ,"attachment;filename=\"" +finalFileName+ "\"");//下载文件的名称

            // 循环取出流中的数据
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
