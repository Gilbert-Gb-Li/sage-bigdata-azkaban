CREATE TABLE `copyrightcloud_secphoto_user_status`  (
  `id` bigint(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `appVersion` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '8.4.1',
  `app_package_name` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'app包名',
  `resourceKey` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'b32e160e9cf4a7524a4567ee693f2c9237798cdf',
  `user_name` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '主播名字',
  `user_id` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '主播id',
  `user_dp` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '主播头像',
  `user_follow_num` bigint(20) DEFAULT NULL COMMENT '用户关注数',
  `user_fans_num` bigint(20) DEFAULT NULL COMMENT '用户粉丝数',
  `user_video_num` bigint(20) DEFAULT NULL COMMENT '用户作品数',
  `new_user` int(2) DEFAULT NULL COMMENT '是否新增用户',
  `increase_user` int(2) DEFAULT NULL COMMENT '是否粉丝暴增用户',
  `user_status` int(2) DEFAULT NULL COMMENT '是否违规用户',
  `user_pure` int(2) DEFAULT NULL COMMENT '是否白名单用户',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_shop_id`(`user_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 6050563 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

SET FOREIGN_KEY_CHECKS = 1;