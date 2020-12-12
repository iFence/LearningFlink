/*
 Navicat Premium Data Transfer

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 50720
 Source Host           : localhost:3306
 Source Schema         : aspirin

 Target Server Type    : MySQL
 Target Server Version : 50720
 File Encoding         : 65001

 Date: 14/11/2020 18:14:24
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for connection
-- ----------------------------
CREATE TABLE IF NOT EXISTS `connection` (
  `id` bigint(20) NOT NULL COMMENT '主键',
  `type` varchar(100) DEFAULT NULL COMMENT '系统类型',
  `url` varchar(100) DEFAULT NULL COMMENT '连接地址',
  `config` varchar(100) DEFAULT NULL COMMENT '配置信息',
  `database` varchar(100) DEFAULT 'FAKEDB' COMMENT '数据库，index',
  `username` varchar(100) DEFAULT NULL COMMENT '用户名',
  `password` varchar(100) DEFAULT NULL COMMENT '密码',
  `desc` varchar(100) DEFAULT NULL COMMENT '描述信息',
  `create_by` varchar(100) DEFAULT NULL COMMENT '创建者',
  `create_time` varchar(100) DEFAULT NULL COMMENT '创建时间',
  `update_by` varchar(100) DEFAULT NULL COMMENT '更新者',
  `update_time` varchar(100) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for user
-- ----------------------------
CREATE TABLE IF NOT EXISTS `user` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `username` varchar(50) DEFAULT NULL COMMENT '用户名',
  `password` varchar(100) DEFAULT NULL COMMENT '密码',
  `role` varchar(10) DEFAULT NULL COMMENT '角色类型',
  `create_by` varchar(20) DEFAULT NULL COMMENT '创建者',
  `create_time` varchar(20) DEFAULT NULL COMMENT '创建时间',
  `update_by` varchar(20) DEFAULT NULL COMMENT '更新者',
  `update_time` varchar(20) DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;

SET FOREIGN_KEY_CHECKS = 1;
