INSERT INTO ob_stat_time_area (datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","edcdb","中国","北京","edcdb","14","1","1","1","1","1","0","0");
INSERT INTO ob_stat_time_area (datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","cadec","中国","北京","cadec","7","1","1","1","1","1","0","1");
INSERT INTO ob_stat_time_area (datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","ebacd","中国","北京","ebacd","10","1","1","1","1","1","0","1");
INSERT INTO ob_stat_time_area (datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","cbbae","中国","北京","cbbae","9","1","1","1","1","1","0","0");
INSERT INTO ob_stat_time_area (datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","dddbc","中国","北京","dddbc","15","1","1","1","1","1","0","1");
INSERT INTO ob_stat_time_area (datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","bdbbe","中国","北京","bdbbe","1","1","1","1","1","1","0","1");
INSERT INTO ob_stat_time_area (datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","edcdb","中国","北京","edcdb","19","1","1","1","1","1","0","1");
INSERT INTO ob_stat_time_area (datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","aaadd","中国","北京","aaadd","7","1","1","1","1","1","0","0");
INSERT INTO ob_stat_time_area (datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","dcbdb","中国","北京","dcbdb","3","1","1","1","1","1","0","0");
INSERT INTO ob_stat_time_area (datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","bbbac","中国","北京","bbbac","9","1","1","1","1","1","0","0");
UPDATE ob_stat_user SET users=users+1,app_users=app_users+1 WHERE datetime="2020-01-17 17:00:00" AND appid="14";
UPDATE ob_stat_user SET users=users+2,app_users=app_users+2 WHERE datetime="2020-01-17 17:00:00" AND appid="7";
UPDATE ob_stat_user SET users=users+1,app_users=app_users+1 WHERE datetime="2020-01-17 17:00:00" AND appid="10";
UPDATE ob_stat_user SET users=users+2,app_users=app_users+2 WHERE datetime="2020-01-17 17:00:00" AND appid="9";
UPDATE ob_stat_user SET users=users+1,app_users=app_users+1 WHERE datetime="2020-01-17 17:00:00" AND appid="15";
INSERT INTO ob_stat_user(datetime,appid,users,app_users) VALUES("2020-01-17 17:00:00","1","1","1");
INSERT INTO ob_stat_user(datetime,appid,users,app_users) VALUES("2020-01-17 17:00:00","19","1","1");
UPDATE ob_stat_user SET users=users+1,app_users=app_users+1 WHERE datetime="2020-01-17 17:00:00" AND appid="3";
INSERT INTO ob_stat_time (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","edcdb","14","1","1","1","1","1","0","0");
INSERT INTO ob_stat_time (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","cadec","7","1","1","1","1","1","0","1");
INSERT INTO ob_stat_time (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","ebacd","10","1","1","1","1","1","0","1");
INSERT INTO ob_stat_time (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","cbbae","9","1","1","1","1","1","0","0");
INSERT INTO ob_stat_time (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","dddbc","15","1","1","1","1","1","0","1");
INSERT INTO ob_stat_time (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","bdbbe","1","1","1","1","1","1","0","1");
INSERT INTO ob_stat_time (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","edcdb","19","1","1","1","1","1","0","1");
INSERT INTO ob_stat_time (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","aaadd","7","1","1","1","1","1","0","0");
INSERT INTO ob_stat_time (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","dcbdb","3","1","1","1","1","1","0","0");
INSERT INTO ob_stat_time (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES ("2020-01-17 17:00:00","bbbac","9","1","1","1","1","1","0","0");