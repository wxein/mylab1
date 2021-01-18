import { Component } from '@angular/core';
import {HttpClient} from '@angular/common/http';

declare const $:any;

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  constructor(private httpClient: HttpClient) { 
    httpClient.get('http://localhost:8080/file').subscribe(success, error);
    
  }
  // ngOnInit() {
  //   $.fn.zTree.init($("#ztree"), this.setting, this.zNodes);
  // }

  //constructor(private httpClient: HttpClient) { 
  //  httpClient.get('http://localhost:8080/file').subscribe(success, error);
  //}
  // constructor(private httpClient: HttpClient) { 
  //   httpClient.get('http://localhost:8080/file').subscribe(success, error);
  // }

  // title = 'filedrive-front';
}
function success(data) {
	var zNode;
  var setting = {
    data: {
      key: {title: "code"},
      simpleData:{
        enable: true
      }
    },
    check: {
      enable: false
    }
  };
  zNode = data;
  $.fn.zTree.init($("#ztree"), setting, zNode);
  console.log('请求成功');
  console.log(data);
}

/**
 * 在控制台打印传入值
 * @param data 任意数据
 */
function error(data) {
  console.log('请求失败');
  console.log(data);
}