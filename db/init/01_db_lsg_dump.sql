CREATE DATABASE  IF NOT EXISTS `db_lsg` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `db_lsg`;
-- MySQL dump 10.13  Distrib 8.0.32, for Win64 (x86_64)
--
-- Host: localhost    Database: db_lsg
-- ------------------------------------------------------
-- Server version	8.0.43

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `adquired_subattribute`
--

DROP TABLE IF EXISTS `adquired_subattribute`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `adquired_subattribute` (
  `id_adquired_subattribute` int NOT NULL AUTO_INCREMENT,
  `id_players` int NOT NULL,
  `id_subattributes_conversion_sensor_endpoint` int NOT NULL,
  `data` int DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_adquired_subattribute`),
  KEY `fk_as_player` (`id_players`),
  KEY `fk_as_scse` (`id_subattributes_conversion_sensor_endpoint`),
  CONSTRAINT `fk_as_player` FOREIGN KEY (`id_players`) REFERENCES `players` (`id_players`),
  CONSTRAINT `fk_as_scse` FOREIGN KEY (`id_subattributes_conversion_sensor_endpoint`) REFERENCES `subattributes_conversion_sensor_endpoint` (`id_subattributes_conversion_sensor_endpoint`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `adquired_subattribute`
--

LOCK TABLES `adquired_subattribute` WRITE;
/*!40000 ALTER TABLE `adquired_subattribute` DISABLE KEYS */;
INSERT INTO `adquired_subattribute` VALUES (1,26,1,8000,'2025-12-10 07:35:00'),(2,26,2,420,'2025-12-10 23:45:00'),(3,22,1,6000,'2025-12-09 08:10:00'),(4,24,1,10000,'2025-12-09 19:20:00');
/*!40000 ALTER TABLE `adquired_subattribute` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `attributes`
--

DROP TABLE IF EXISTS `attributes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `attributes` (
  `id_attributes` int NOT NULL AUTO_INCREMENT,
  `name` varchar(45) DEFAULT NULL,
  `description` varchar(300) DEFAULT NULL,
  `data_type` varchar(45) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id_attributes`),
  UNIQUE KEY `uq_attr_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `attributes`
--

LOCK TABLES `attributes` WRITE;
/*!40000 ALTER TABLE `attributes` DISABLE KEYS */;
INSERT INTO `attributes` VALUES (1,'Social','placeholder','placeholder','2022-04-20 03:14:07','2022-04-20 03:14:07'),(2,'Fisico','placeholder','placeholder','2022-04-20 03:14:07','2022-04-20 03:14:07'),(3,'Afectivo','placeholder','placeholder','2022-04-20 03:14:07','2022-04-20 03:14:07'),(4,'Mental','placeholder','placeholder','2022-04-20 03:14:07','2022-04-20 03:14:07'),(5,'Linguistico','placeholder','placeholder','2022-04-20 03:14:07','2022-04-20 03:14:07');
/*!40000 ALTER TABLE `attributes` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_attributes_ins` AFTER INSERT ON `attributes` FOR EACH ROW BEGIN
  INSERT INTO audit_log(table_name, op, row_pk, changed_by, new_row)
  VALUES (
    'attributes',
    'INSERT',
    CAST(NEW.id_attributes AS CHAR),
    COALESCE(@app_user, CURRENT_USER()),
    JSON_OBJECT(
      'id_attributes', NEW.id_attributes,
      'name',         NEW.name,
      'description',  NEW.description,
      'data_type',    NEW.data_type,
      'created_at',   NEW.created_at,
      'updated_at',   NEW.updated_at
    )
  );
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_attributes_upd` AFTER UPDATE ON `attributes` FOR EACH ROW BEGIN
  INSERT INTO audit_log(table_name, op, row_pk, changed_by, old_row, new_row)
  VALUES (
    'attributes',
    'UPDATE',
    CAST(NEW.id_attributes AS CHAR),
    COALESCE(@app_user, CURRENT_USER()),
    JSON_OBJECT(
      'id_attributes', OLD.id_attributes,
      'name',         OLD.name,
      'description',  OLD.description,
      'data_type',    OLD.data_type,
      'created_at',   OLD.created_at,
      'updated_at',   OLD.updated_at
    ),
    JSON_OBJECT(
      'id_attributes', NEW.id_attributes,
      'name',         NEW.name,
      'description',  NEW.description,
      'data_type',    NEW.data_type,
      'created_at',   NEW.created_at,
      'updated_at',   NEW.updated_at
    )
  );
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_attributes_del` AFTER DELETE ON `attributes` FOR EACH ROW BEGIN
  INSERT INTO audit_log(table_name, op, row_pk, changed_by, old_row)
  VALUES (
    'attributes',
    'DELETE',
    CAST(OLD.id_attributes AS CHAR),
    COALESCE(@app_user, CURRENT_USER()),
    JSON_OBJECT(
      'id_attributes', OLD.id_attributes,
      'name',         OLD.name,
      'description',  OLD.description,
      'data_type',    OLD.data_type,
      'created_at',   OLD.created_at,
      'updated_at',   OLD.updated_at
    )
  );
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `audit_log`
--

DROP TABLE IF EXISTS `audit_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `audit_log` (
  `id_audit_log` bigint NOT NULL AUTO_INCREMENT,
  `table_name` varchar(64) NOT NULL,
  `op` enum('INSERT','UPDATE','DELETE') NOT NULL,
  `row_pk` varchar(128) NOT NULL,
  `changed_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `changed_by` varchar(128) DEFAULT NULL,
  `old_row` json DEFAULT NULL,
  `new_row` json DEFAULT NULL,
  PRIMARY KEY (`id_audit_log`),
  KEY `ix_audit_table_time` (`table_name`,`changed_at`),
  KEY `ix_audit_op_time` (`op`,`changed_at`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `audit_log`
--

LOCK TABLES `audit_log` WRITE;
/*!40000 ALTER TABLE `audit_log` DISABLE KEYS */;
INSERT INTO `audit_log` VALUES (1,'attributes','DELETE','8','2025-12-11 18:53:50','system','{\"name\": \"auto_attr_0\", \"data_type\": null, \"created_at\": null, \"updated_at\": null, \"description\": null, \"id_attributes\": 8}',NULL),(2,'attributes','DELETE','6','2025-12-11 20:50:04','system','{\"name\": \"pendiente_nombre\", \"data_type\": null, \"created_at\": null, \"updated_at\": null, \"description\": null, \"id_attributes\": 6}',NULL),(3,'attributes','DELETE','7','2025-12-11 20:50:12','system','{\"name\": \"auto_attr_0\", \"data_type\": null, \"created_at\": null, \"updated_at\": null, \"description\": null, \"id_attributes\": 7}',NULL);
/*!40000 ALTER TABLE `audit_log` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `conversion`
--

DROP TABLE IF EXISTS `conversion`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `conversion` (
  `id_conversion` int NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `description` varchar(300) DEFAULT NULL,
  `operations` varchar(200) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id_conversion`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `conversion`
--

LOCK TABLES `conversion` WRITE;
/*!40000 ALTER TABLE `conversion` DISABLE KEYS */;
INSERT INTO `conversion` VALUES (1,'conversion','conversion','placeholder','2022-04-20 03:14:07','2022-04-20 03:14:07');
/*!40000 ALTER TABLE `conversion` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `expended_attribute`
--

DROP TABLE IF EXISTS `expended_attribute`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `expended_attribute` (
  `id_expended_attribute` int NOT NULL AUTO_INCREMENT,
  `id_players` int NOT NULL,
  `id_videogame` int unsigned NOT NULL,
  `id_modifiable_conversion_attribute` int NOT NULL,
  `data` int NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id_expended_attribute`),
  KEY `fk_ea_player` (`id_players`),
  KEY `fk_ea_game` (`id_videogame`),
  KEY `fk_ea_mca` (`id_modifiable_conversion_attribute`),
  CONSTRAINT `fk_ea_game` FOREIGN KEY (`id_videogame`) REFERENCES `videogame` (`id_videogame`),
  CONSTRAINT `fk_ea_mca` FOREIGN KEY (`id_modifiable_conversion_attribute`) REFERENCES `modifiable_conversion_attribute` (`id_modifiable_conversion_attribute`),
  CONSTRAINT `fk_ea_player` FOREIGN KEY (`id_players`) REFERENCES `players` (`id_players`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `expended_attribute`
--

LOCK TABLES `expended_attribute` WRITE;
/*!40000 ALTER TABLE `expended_attribute` DISABLE KEYS */;
INSERT INTO `expended_attribute` VALUES (1,26,1,1,30,'2025-12-10 19:10:00'),(2,24,1,1,50,'2025-12-09 21:00:00');
/*!40000 ALTER TABLE `expended_attribute` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `lsg_game_session`
--

DROP TABLE IF EXISTS `lsg_game_session`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `lsg_game_session` (
  `id_lsg_game_session` bigint NOT NULL AUTO_INCREMENT,
  `id_player_videogame` int NOT NULL,
  `started_at` timestamp NOT NULL,
  `ended_at` timestamp NULL DEFAULT NULL,
  `session_metrics` json DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `duration_seconds` int GENERATED ALWAYS AS ((case when (`ended_at` is null) then NULL else timestampdiff(SECOND,`started_at`,`ended_at`) end)) STORED,
  PRIMARY KEY (`id_lsg_game_session`),
  KEY `ix_session_pvg_start` (`id_player_videogame`,`started_at`),
  KEY `ix_session_time` (`started_at`),
  KEY `ix_session_pvg_end` (`id_player_videogame`,`ended_at`),
  CONSTRAINT `fk_sess_pvg` FOREIGN KEY (`id_player_videogame`) REFERENCES `player_videogame` (`id_player_videogame`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `lsg_game_session`
--

LOCK TABLES `lsg_game_session` WRITE;
/*!40000 ALTER TABLE `lsg_game_session` DISABLE KEYS */;
INSERT INTO `lsg_game_session` (`id_lsg_game_session`, `id_player_videogame`, `started_at`, `ended_at`, `session_metrics`, `created_at`) VALUES (1,1,'2025-12-10 18:00:00','2025-12-10 19:30:00','{\"events\": 120, \"redeems\": 1}','2025-12-10 19:30:05'),(2,1,'2025-12-09 20:00:00','2025-12-09 21:15:00','{\"events\": 90, \"redeems\": 0}','2025-12-09 21:15:10'),(3,3,'2025-12-09 21:30:00','2025-12-09 22:10:00','{\"events\": 60, \"redeems\": 1}','2025-12-09 22:10:05'),(4,5,'2025-12-09 20:00:00','2025-12-09 21:45:00','{\"events\": 110, \"redeems\": 2}','2025-12-09 21:45:10');
/*!40000 ALTER TABLE `lsg_game_session` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_lsg_game_session_del` AFTER DELETE ON `lsg_game_session` FOR EACH ROW BEGIN
  INSERT INTO audit_log (table_name, op, row_pk, changed_by, old_row)
  VALUES (
    'lsg_game_session',
    'DELETE',
    CAST(OLD.id_lsg_game_session AS CHAR),
    COALESCE(@app_user, CURRENT_USER()),
    JSON_OBJECT(
      'id_lsg_game_session', OLD.id_lsg_game_session,
      'id_player_videogame', OLD.id_player_videogame,
      'started_at',          OLD.started_at,
      'ended_at',            OLD.ended_at,
      'duration_seconds',    OLD.duration_seconds,
      'session_metrics',     OLD.session_metrics,
      'created_at',          OLD.created_at
    )
  );
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `modifiable_conversion_attribute`
--

DROP TABLE IF EXISTS `modifiable_conversion_attribute`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `modifiable_conversion_attribute` (
  `id_modifiable_conversion_attribute` int NOT NULL AUTO_INCREMENT,
  `id_attribute` int NOT NULL,
  `id_conversion` int NOT NULL,
  `id_modifiable_mechanic` int NOT NULL,
  PRIMARY KEY (`id_modifiable_conversion_attribute`),
  KEY `fk_mca_attr` (`id_attribute`),
  KEY `fk_mca_conv` (`id_conversion`),
  KEY `fk_mca_mech` (`id_modifiable_mechanic`),
  CONSTRAINT `fk_mca_attr` FOREIGN KEY (`id_attribute`) REFERENCES `attributes` (`id_attributes`),
  CONSTRAINT `fk_mca_conv` FOREIGN KEY (`id_conversion`) REFERENCES `conversion` (`id_conversion`),
  CONSTRAINT `fk_mca_mech` FOREIGN KEY (`id_modifiable_mechanic`) REFERENCES `modifiable_mechanic` (`id_modifiable_mechanic`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `modifiable_conversion_attribute`
--

LOCK TABLES `modifiable_conversion_attribute` WRITE;
/*!40000 ALTER TABLE `modifiable_conversion_attribute` DISABLE KEYS */;
INSERT INTO `modifiable_conversion_attribute` VALUES (1,2,1,1),(2,3,1,1);
/*!40000 ALTER TABLE `modifiable_conversion_attribute` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `modifiable_mechanic`
--

DROP TABLE IF EXISTS `modifiable_mechanic`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `modifiable_mechanic` (
  `id_modifiable_mechanic` int NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `description` varchar(200) DEFAULT NULL,
  `type` enum('buff','nerf','speed','health','economy') DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id_modifiable_mechanic`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `modifiable_mechanic`
--

LOCK TABLES `modifiable_mechanic` WRITE;
/*!40000 ALTER TABLE `modifiable_mechanic` DISABLE KEYS */;
INSERT INTO `modifiable_mechanic` VALUES (1,'Faster Peasants','placeholder','nerf','2022-04-20 03:14:07','2022-04-20 03:14:07');
/*!40000 ALTER TABLE `modifiable_mechanic` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `modifiable_mechanic_videogames`
--

DROP TABLE IF EXISTS `modifiable_mechanic_videogames`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `modifiable_mechanic_videogames` (
  `id_modifiable_mechanic_videogame` int NOT NULL AUTO_INCREMENT,
  `id_modifiable_mechanic` int NOT NULL,
  `id_videogame` int unsigned DEFAULT NULL,
  `options` json DEFAULT NULL,
  PRIMARY KEY (`id_modifiable_mechanic_videogame`),
  KEY `fk_mmv_game` (`id_videogame`),
  KEY `fk_mmv_mech` (`id_modifiable_mechanic`),
  CONSTRAINT `fk_mmv_game` FOREIGN KEY (`id_videogame`) REFERENCES `videogame` (`id_videogame`),
  CONSTRAINT `fk_mmv_mech` FOREIGN KEY (`id_modifiable_mechanic`) REFERENCES `modifiable_mechanic` (`id_modifiable_mechanic`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `modifiable_mechanic_videogames`
--

LOCK TABLES `modifiable_mechanic_videogames` WRITE;
/*!40000 ALTER TABLE `modifiable_mechanic_videogames` DISABLE KEYS */;
INSERT INTO `modifiable_mechanic_videogames` VALUES (1,1,1,'null');
/*!40000 ALTER TABLE `modifiable_mechanic_videogames` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `online_sensor`
--

DROP TABLE IF EXISTS `online_sensor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `online_sensor` (
  `id_online_sensor` int NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `description` varchar(200) DEFAULT NULL,
  `base_url` varchar(1000) DEFAULT NULL,
  `initiated_date` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id_online_sensor`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `online_sensor`
--

LOCK TABLES `online_sensor` WRITE;
/*!40000 ALTER TABLE `online_sensor` DISABLE KEYS */;
INSERT INTO `online_sensor` VALUES (1,'Fitbit Demo','API ficticia para pasos diarios','https://api.fitbit.local','2025-12-11 21:23:38','2025-12-11 21:23:38'),(2,'SleepTracker Demo','API ficticia para horas de sueño','https://api.sleep.local','2025-12-11 21:23:38','2025-12-11 21:23:38');
/*!40000 ALTER TABLE `online_sensor` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `player_online_sensor`
--

DROP TABLE IF EXISTS `player_online_sensor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `player_online_sensor` (
  `id_players_online_sensor` int NOT NULL AUTO_INCREMENT,
  `id_players` int NOT NULL,
  `id_online_sensor` int NOT NULL,
  `tokens` json DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `rotated_at` timestamp NULL DEFAULT NULL,
  `expires_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id_players_online_sensor`),
  UNIQUE KEY `uq_pos` (`id_players`,`id_online_sensor`),
  KEY `fk_pos_online` (`id_online_sensor`),
  KEY `ix_pos_exp` (`expires_at`),
  CONSTRAINT `fk_pos_online` FOREIGN KEY (`id_online_sensor`) REFERENCES `online_sensor` (`id_online_sensor`),
  CONSTRAINT `fk_pos_player` FOREIGN KEY (`id_players`) REFERENCES `players` (`id_players`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `player_online_sensor`
--

LOCK TABLES `player_online_sensor` WRITE;
/*!40000 ALTER TABLE `player_online_sensor` DISABLE KEYS */;
INSERT INTO `player_online_sensor` VALUES (1,26,1,'{\"access_token\": \"demo_token_fitbit_26\"}','2025-12-11 21:23:54','2025-12-11 21:23:54','2025-12-11 21:23:54','2026-01-10 21:23:54'),(2,26,2,'{\"access_token\": \"demo_token_sleep_26\"}','2025-12-11 21:23:54','2025-12-11 21:23:54','2025-12-11 21:23:54','2026-01-10 21:23:54'),(3,22,1,'{\"access_token\": \"demo_token_fitbit_22\"}','2025-12-11 21:23:54','2025-12-11 21:23:54','2025-12-11 21:23:54','2026-01-10 21:23:54'),(4,22,2,'{\"access_token\": \"demo_token_sleep_22\"}','2025-12-11 21:23:54','2025-12-11 21:23:54','2025-12-11 21:23:54','2026-01-10 21:23:54'),(5,24,1,'{\"access_token\": \"demo_token_fitbit_24\"}','2025-12-11 21:23:54','2025-12-11 21:23:54','2025-12-11 21:23:54','2026-01-10 21:23:54'),(6,24,2,'{\"access_token\": \"demo_token_sleep_24\"}','2025-12-11 21:23:54','2025-12-11 21:23:54','2025-12-11 21:23:54','2026-01-10 21:23:54');
/*!40000 ALTER TABLE `player_online_sensor` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `player_videogame`
--

DROP TABLE IF EXISTS `player_videogame`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `player_videogame` (
  `id_player_videogame` int NOT NULL AUTO_INCREMENT,
  `id_players` int NOT NULL,
  `id_videogame` int unsigned NOT NULL,
  `lsg_enabled` tinyint(1) NOT NULL DEFAULT '1',
  `first_seen` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `last_seen` timestamp NULL DEFAULT NULL,
  `plugin_version` varchar(32) DEFAULT NULL,
  `settings` json DEFAULT NULL,
  PRIMARY KEY (`id_player_videogame`),
  UNIQUE KEY `uq_player_game` (`id_players`,`id_videogame`),
  KEY `ix_pvg_player_game` (`id_players`,`id_videogame`),
  KEY `fk_pvg_game` (`id_videogame`),
  CONSTRAINT `fk_pvg_game` FOREIGN KEY (`id_videogame`) REFERENCES `videogame` (`id_videogame`),
  CONSTRAINT `fk_pvg_player` FOREIGN KEY (`id_players`) REFERENCES `players` (`id_players`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `player_videogame`
--

LOCK TABLES `player_videogame` WRITE;
/*!40000 ALTER TABLE `player_videogame` DISABLE KEYS */;
INSERT INTO `player_videogame` VALUES (1,26,1,1,'2025-12-01 18:00:00','2025-12-10 19:30:00','1.0.0','{\"difficulty\": \"normal\"}'),(2,26,14,1,'2025-12-02 20:00:00','2025-12-10 22:00:00','1.0.0','{\"city\": \"Santiago\"}'),(3,22,14,1,'2025-12-05 21:00:00','2025-12-10 21:30:00','1.0.0','{\"city\": \"UpperTown\"}'),(4,22,15,1,'2025-12-06 19:00:00','2025-12-10 20:00:00','1.0.0','{\"world\": \"Alpha\"}'),(5,24,1,1,'2025-12-03 17:00:00','2025-12-09 22:00:00','1.0.0','{\"difficulty\": \"hard\"}');
/*!40000 ALTER TABLE `player_videogame` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `players`
--

DROP TABLE IF EXISTS `players`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `players` (
  `id_players` int NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL,
  `password_hash` char(95) DEFAULT NULL,
  `email` varchar(128) NOT NULL,
  `role` enum('player', 'teacher', 'researcher', 'admin') NOT NULL DEFAULT 'player',
  `age` int DEFAULT NULL,
  `external_type` varchar(16) DEFAULT NULL,
  `external_id` varchar(128) DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_players`),
  UNIQUE KEY `uq_playerss_email` (`email`),
  CONSTRAINT `chk_players_auth` CHECK ((((`password_hash` is not null) and (`external_type` is null) and (`external_id` is null)) or ((`password_hash` is null) and (`external_type` is not null) and (`external_id` is not null)) or ((`password_hash` is null) and (`external_type` is null) and (`external_id` is null))))
) ENGINE=InnoDB AUTO_INCREMENT=34 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `players`
--

LOCK TABLES `players` WRITE;
/*!40000 ALTER TABLE `players` DISABLE KEYS */;
INSERT INTO `players` VALUES (22,'aaldea','$2b$12$7XGc7hCL.QFeYFpUaTlsyejnydI5RZ.aetBojx.vLqBWx9eKVDOgi','alejandro.aldea@usach.cl', 'player',0,NULL,NULL,'2025-12-11 20:42:33','2025-12-11 20:42:33'),(24,'hherrera','$2b$12$ND2fcMCIKgsztTdoLM3lvukwXuMsvE7yg//M93iZsBogSgNhnbaNK','hernan.herrera@usach.cl', 'player',0,NULL,NULL,'2025-12-11 20:43:24','2025-12-11 20:43:24'),(25,'lmellado','$2b$12$v2LZnXLHSpyQnj1rLLP.3OefgDCdCgd3eH.LeDx0dmp6W0/PjOrl6','luis.mellado.v@usach.cl', 'player',0,NULL,NULL,'2025-12-11 20:44:02','2025-12-11 20:44:02'),(26,'jmacias','$2b$12$8qLpu/Oeb0c51.bGz4meteOwxQbinAyLBg7JSJPvnkWfRmf46y9mC','joaquin.macias@usach.cl', 'admin',35,NULL,NULL,'2025-12-11 20:44:31','2025-12-11 20:44:31'),(27,'rgonzalez','$2b$12$wb.3ssYQQNxo8slkYT3j2.Yc4v3Q9PKZzr5Y3hV0hebpmhGLuTdme','roberto.gonzalez.i@usach.cl', 'player',0,NULL,NULL,'2025-12-11 20:44:59','2025-12-11 20:44:59'),(28,'wjimenez','$2b$12$yQ4W5OLZiU1TrGuFhIfjH.O9SnD8ujhEUAm932ri9TmCv9gAJgulq','williams.jimenez@usach.cl', 'player',0,NULL,NULL,'2025-12-11 20:45:26','2025-12-11 20:45:26'),(30,'acastro','$2b$12$jcjtEMMSOd19kM4Y2hEHrumg55LZ4smeLaPotjL.3jd7zzO84slSO','aracely.castro@usach.cl', 'player',0,NULL,NULL,'2025-12-11 20:46:01','2025-12-11 20:46:01'),(31,'ravaca','$2b$12$9E2qjhPik7BqcQSR.UW0meUJJHfb0ouIBuHL07FzbofPIgDeCom7W','ricardo.avaca@usach.cl', 'player',0,NULL,NULL,'2025-12-11 20:46:25','2025-12-11 20:46:25'),(32,'erodriguez','$2b$12$89KYMJ6mZLlwPFuKTc0bgOdQk/quNkpcfU5hYvPPt72wxkAnfUI4q','enrique.rodriguez-lapuente@usach.cl', 'player',0,NULL,NULL,'2025-12-11 20:46:47','2025-12-11 20:46:47'),(33,'ngabrielli','$2b$12$6VqoO4Hn7H5On1.YGlN/n.W3ycq/xA9/VY6kch41KXXVvx8DVQVaG','nicolas.gabrielli@usach.cl', 'player',0,NULL,NULL,'2025-12-11 20:47:16','2025-12-11 20:47:16');
/*!40000 ALTER TABLE `players` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_players_bi` BEFORE INSERT ON `players` FOR EACH ROW BEGIN
  IF NOT (
       (NEW.password_hash IS NOT NULL AND NEW.external_type IS NULL AND NEW.external_id IS NULL)
    OR (NEW.password_hash IS NULL     AND NEW.external_type IS NOT NULL AND NEW.external_id IS NOT NULL)
  ) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Regla XOR de autenticación violada (players)';
  END IF;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_players_bu` BEFORE UPDATE ON `players` FOR EACH ROW BEGIN
  IF NOT (
       (NEW.password_hash IS NOT NULL AND NEW.external_type IS NULL AND NEW.external_id IS NULL)
    OR (NEW.password_hash IS NULL     AND NEW.external_type IS NOT NULL AND NEW.external_id IS NOT NULL)
  ) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Regla XOR de autenticación violada (players)';
  END IF;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_players_del` AFTER DELETE ON `players` FOR EACH ROW BEGIN
  INSERT INTO audit_log (table_name, op, row_pk, changed_by, old_row)
  VALUES (
    'players',
    'DELETE',
    CAST(OLD.id_players AS CHAR),
    COALESCE(@app_user, CURRENT_USER()),
    JSON_OBJECT(
      'id_players',      OLD.id_players,
      'name',            OLD.name,
      'email',           OLD.email,
      'age',             OLD.age,
      'external_type',   OLD.external_type,
      'external_id',     OLD.external_id,
      'created_at',      OLD.created_at,
      'updated_at',      OLD.updated_at
    )
  );
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `players_attributes`
--

DROP TABLE IF EXISTS `players_attributes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `players_attributes` (
  `id_players_attributes` int NOT NULL AUTO_INCREMENT,
  `id_players` int NOT NULL,
  `id_attributes` int NOT NULL,
  `data` int DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_players_attributes`),
  UNIQUE KEY `uq_pa` (`id_players`,`id_attributes`),
  KEY `ix_pa_player` (`id_players`),
  KEY `ix_pa_attr` (`id_attributes`),
  KEY `ix_pa_attr_player` (`id_attributes`,`id_players`),
  CONSTRAINT `fk_pa_attr` FOREIGN KEY (`id_attributes`) REFERENCES `attributes` (`id_attributes`),
  CONSTRAINT `fk_pa_player` FOREIGN KEY (`id_players`) REFERENCES `players` (`id_players`)
) ENGINE=InnoDB AUTO_INCREMENT=152 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `players_attributes`
--

LOCK TABLES `players_attributes` WRITE;
/*!40000 ALTER TABLE `players_attributes` DISABLE KEYS */;
INSERT INTO `players_attributes` VALUES (76,22,3,0,'2025-12-11 21:01:30'),(77,22,2,60,'2025-12-11 21:24:48'),(78,22,5,0,'2025-12-11 21:01:30'),(79,22,4,0,'2025-12-11 21:01:30'),(80,22,1,0,'2025-12-11 21:01:30'),(83,24,3,0,'2025-12-11 21:02:09'),(84,24,2,50,'2025-12-11 21:24:48'),(85,24,5,0,'2025-12-11 21:02:09'),(86,24,4,0,'2025-12-11 21:02:09'),(87,24,1,0,'2025-12-11 21:02:09'),(90,25,3,0,'2025-12-11 21:02:14'),(91,25,2,0,'2025-12-11 21:02:14'),(92,25,5,0,'2025-12-11 21:02:14'),(93,25,4,0,'2025-12-11 21:02:14'),(94,25,1,0,'2025-12-11 21:02:14'),(97,26,3,40,'2025-12-11 21:24:48'),(98,26,2,50,'2025-12-11 21:24:48'),(99,26,5,0,'2025-12-11 21:02:18'),(100,26,4,0,'2025-12-11 21:02:18'),(101,26,1,0,'2025-12-11 21:02:18'),(104,27,3,0,'2025-12-11 21:02:22'),(105,27,2,0,'2025-12-11 21:02:22'),(106,27,5,0,'2025-12-11 21:02:22'),(107,27,4,0,'2025-12-11 21:02:22'),(108,27,1,0,'2025-12-11 21:02:22'),(111,28,3,0,'2025-12-11 21:02:26'),(112,28,2,0,'2025-12-11 21:02:26'),(113,28,5,0,'2025-12-11 21:02:26'),(114,28,4,0,'2025-12-11 21:02:26'),(115,28,1,0,'2025-12-11 21:02:26'),(118,30,3,0,'2025-12-11 21:02:31'),(119,30,2,0,'2025-12-11 21:02:31'),(120,30,5,0,'2025-12-11 21:02:31'),(121,30,4,0,'2025-12-11 21:02:31'),(122,30,1,0,'2025-12-11 21:02:31'),(125,31,3,0,'2025-12-11 21:02:36'),(126,31,2,0,'2025-12-11 21:02:36'),(127,31,5,0,'2025-12-11 21:02:36'),(128,31,4,0,'2025-12-11 21:02:36'),(129,31,1,0,'2025-12-11 21:02:36'),(132,32,3,0,'2025-12-11 21:02:41'),(133,32,2,0,'2025-12-11 21:02:41'),(134,32,5,0,'2025-12-11 21:02:41'),(135,32,4,0,'2025-12-11 21:02:41'),(136,32,1,0,'2025-12-11 21:02:41'),(139,33,3,0,'2025-12-11 21:02:42'),(140,33,2,0,'2025-12-11 21:02:42'),(141,33,5,0,'2025-12-11 21:02:42'),(142,33,4,0,'2025-12-11 21:02:42'),(143,33,1,0,'2025-12-11 21:02:42');
/*!40000 ALTER TABLE `players_attributes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `players_sensor_endpoint`
--

DROP TABLE IF EXISTS `players_sensor_endpoint`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `players_sensor_endpoint` (
  `id_players_sensor_endpoint` int NOT NULL AUTO_INCREMENT,
  `id_players` int NOT NULL,
  `Id_sensor_endpoint` int NOT NULL,
  `specific_parameters` json DEFAULT NULL,
  `activated` tinyint(1) DEFAULT NULL,
  `schedule_time` int DEFAULT NULL,
  PRIMARY KEY (`id_players_sensor_endpoint`),
  KEY `ix_pse_player` (`id_players`),
  KEY `ix_pse_endpoint` (`Id_sensor_endpoint`),
  CONSTRAINT `fk_pse_endpoint` FOREIGN KEY (`Id_sensor_endpoint`) REFERENCES `sensor_endpoint` (`id_sensor_endpoint`),
  CONSTRAINT `fk_pse_player` FOREIGN KEY (`id_players`) REFERENCES `players` (`id_players`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `players_sensor_endpoint`
--

LOCK TABLES `players_sensor_endpoint` WRITE;
/*!40000 ALTER TABLE `players_sensor_endpoint` DISABLE KEYS */;
INSERT INTO `players_sensor_endpoint` VALUES (1,26,1,'{\"timezone\": \"America/Santiago\"}',1,15),(2,26,2,'{\"timezone\": \"America/Santiago\"}',1,60),(3,22,1,'{\"timezone\": \"America/Santiago\"}',1,30),(4,22,2,'{\"timezone\": \"America/Santiago\"}',1,60),(5,24,1,'{\"timezone\": \"America/Santiago\"}',1,20),(6,24,2,'{\"timezone\": \"America/Santiago\"}',1,60);
/*!40000 ALTER TABLE `players_sensor_endpoint` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `point_dimension`
--

DROP TABLE IF EXISTS `point_dimension`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `point_dimension` (
  `id_point_dimension` int NOT NULL AUTO_INCREMENT,
  `id_attributes` int DEFAULT NULL,
  `id_subattributes` int DEFAULT NULL,
  `code` varchar(64) NOT NULL,
  `name` varchar(128) NOT NULL,
  PRIMARY KEY (`id_point_dimension`),
  UNIQUE KEY `uq_point_dimension_code` (`code`),
  KEY `fk_pd_attr` (`id_attributes`),
  KEY `fk_pd_sub` (`id_subattributes`),
  CONSTRAINT `fk_pd_attr` FOREIGN KEY (`id_attributes`) REFERENCES `attributes` (`id_attributes`),
  CONSTRAINT `fk_pd_sub` FOREIGN KEY (`id_subattributes`) REFERENCES `subattributes` (`id_subattributes`),
  CONSTRAINT `chk_pd_one` CHECK (((`id_attributes` is not null) xor (`id_subattributes` is not null)))
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `point_dimension`
--

LOCK TABLES `point_dimension` WRITE;
/*!40000 ALTER TABLE `point_dimension` DISABLE KEYS */;
INSERT INTO `point_dimension` VALUES (1,1,NULL,'S/I','S/I'),(2,2,NULL,'FISICO_BASE','Puntos de actividad física'),(3,3,NULL,'AFECTIVO_BASE','Puntos de bienestar afectivo'),(4,4,NULL,'MENTAL_BASE','Puntos de desarrollo mental'),(5,NULL,6,'CONDICION_FISICA','Condición física (subatributo)'),(6,NULL,11,'REG_EMOCIONAL','Regulación emocional (subatributo)');
/*!40000 ALTER TABLE `point_dimension` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `points_ledger`
--

DROP TABLE IF EXISTS `points_ledger`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `points_ledger` (
  `id_points_ledger` bigint NOT NULL AUTO_INCREMENT,
  `id_players` int NOT NULL,
  `id_point_dimension` int NOT NULL,
  `id_videogame` int unsigned DEFAULT NULL,
  `direction` enum('CREDIT','DEBIT') NOT NULL,
  `amount` int NOT NULL,
  `source_type` enum('SENSOR','API','MANUAL','IMPORT','ADJUST','REDEMPTION') NOT NULL,
  `source_ref` varchar(128) NOT NULL,
  `payload` json DEFAULT NULL,
  `occurred_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id_sensor_ingest_event` bigint DEFAULT NULL,
  PRIMARY KEY (`id_points_ledger`),
  UNIQUE KEY `uq_points_idem` (`id_players`,`source_type`,`source_ref`),
  KEY `ix_points_player_time` (`id_players`,`occurred_at`),
  KEY `ix_points_dimension` (`id_point_dimension`),
  KEY `fk_pl_sie` (`id_sensor_ingest_event`),
  KEY `ix_pl_game_time` (`id_videogame`,`occurred_at`),
  KEY `ix_pl_game_dir_time` (`id_videogame`,`direction`,`occurred_at`),
  CONSTRAINT `fk_pl_dim` FOREIGN KEY (`id_point_dimension`) REFERENCES `point_dimension` (`id_point_dimension`),
  CONSTRAINT `fk_pl_game` FOREIGN KEY (`id_videogame`) REFERENCES `videogame` (`id_videogame`),
  CONSTRAINT `fk_pl_player` FOREIGN KEY (`id_players`) REFERENCES `players` (`id_players`),
  CONSTRAINT `fk_pl_sie` FOREIGN KEY (`id_sensor_ingest_event`) REFERENCES `sensor_ingest_event` (`id_sensor_ingest_event`),
  CONSTRAINT `points_ledger_chk_1` CHECK ((`amount` > 0))
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `points_ledger`
--

LOCK TABLES `points_ledger` WRITE;
/*!40000 ALTER TABLE `points_ledger` DISABLE KEYS */;
INSERT INTO `points_ledger` VALUES (1,26,2,1,'CREDIT',80,'SENSOR','d26d1e08-d6d7-11f0-bb4d-0242ac120002','{\"steps\": 8000, \"reason\": \"steps_to_points\"}','2025-12-10 08:00:00','2025-12-10 08:00:05',1),(2,26,3,1,'CREDIT',40,'SENSOR','d26d3cb4-d6d7-11f0-bb4d-0242ac120002','{\"reason\": \"sleep_to_points\", \"minutes_asleep\": 420}','2025-12-11 00:00:00','2025-12-11 00:00:05',2),(3,26,2,1,'DEBIT',30,'REDEMPTION','redeem_26_uw_1','{\"mechanic\": \"Faster Peasants\"}','2025-12-10 19:05:00','2025-12-10 19:05:05',NULL),(4,22,2,14,'CREDIT',60,'SENSOR','d26d4f4c-d6d7-11f0-bb4d-0242ac120002','{\"steps\": 6000, \"reason\": \"steps_to_points\"}','2025-12-09 08:30:00','2025-12-09 08:30:05',3),(5,24,2,1,'CREDIT',100,'SENSOR','d26d5867-d6d7-11f0-bb4d-0242ac120002','{\"steps\": 10000, \"reason\": \"steps_to_points\"}','2025-12-09 18:45:00','2025-12-09 18:45:05',4),(6,24,2,1,'DEBIT',50,'REDEMPTION','redeem_24_uw_1','{\"mechanic\": \"Faster Peasants\"}','2025-12-09 21:00:00','2025-12-09 21:00:05',NULL);
/*!40000 ALTER TABLE `points_ledger` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_points_ledger_before_ai` BEFORE INSERT ON `points_ledger` FOR EACH ROW BEGIN
  IF NEW.source_ref IS NULL AND NEW.source_type IN ('SENSOR','API','IMPORT','ADJUST') THEN
    SET NEW.source_ref = UUID();
  END IF;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_points_ledger_snapshot_ai` AFTER INSERT ON `points_ledger` FOR EACH ROW BEGIN
  DECLARE v_attr INT;

  /* Determinar el atributo asociado a la dimensión de puntos */
  SELECT COALESCE(pd.id_attributes,
                  (SELECT attributes_id_attributes
                     FROM subattributes
                    WHERE id_subattributes = pd.id_subattributes))
    INTO v_attr
  FROM point_dimension pd
  WHERE pd.id_point_dimension = NEW.id_point_dimension;

  IF v_attr IS NOT NULL THEN
    INSERT INTO players_attributes (id_players, id_attributes, data, updated_at)
    VALUES (
      NEW.id_players,
      v_attr,
      CASE
        WHEN NEW.direction = 'CREDIT' THEN NEW.amount
        ELSE -NEW.amount
      END,
      NOW()
    )
    ON DUPLICATE KEY UPDATE
      data       = data + (CASE
                              WHEN NEW.direction = 'CREDIT' THEN NEW.amount
                              ELSE -NEW.amount
                            END),
      updated_at = NOW();
  END IF;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_points_ledger_del` AFTER DELETE ON `points_ledger` FOR EACH ROW BEGIN
  INSERT INTO audit_log (table_name, op, row_pk, changed_by, old_row)
  VALUES (
    'points_ledger',
    'DELETE',
    CAST(OLD.id_points_ledger AS CHAR),
    COALESCE(@app_user, CURRENT_USER()),
    JSON_OBJECT(
      'id_points_ledger', OLD.id_points_ledger,
      'id_players',       OLD.id_players,
      'id_point_dimension', OLD.id_point_dimension,
      'id_videogame',     OLD.id_videogame,
      'direction',        OLD.direction,
      'amount',           OLD.amount,
      'source_type',      OLD.source_type,
      'source_ref',       OLD.source_ref,
      'payload',          OLD.payload,
      'occurred_at',      OLD.occurred_at,
      'created_at',       OLD.created_at,
      'id_sensor_ingest_event', OLD.id_sensor_ingest_event
    )
  );
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `redemption_event`
--

DROP TABLE IF EXISTS `redemption_event`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `redemption_event` (
  `id_redemption_event` bigint NOT NULL AUTO_INCREMENT,
  `id_points_ledger` bigint NOT NULL,
  `id_modifiable_mechanic_videogame` int NOT NULL,
  `redeemed_points` int NOT NULL,
  `redeemed_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `metadata` json DEFAULT NULL,
  PRIMARY KEY (`id_redemption_event`),
  UNIQUE KEY `uq_re_ledger` (`id_points_ledger`),
  KEY `ix_redeem_time` (`redeemed_at`),
  KEY `fk_re_mmv` (`id_modifiable_mechanic_videogame`),
  CONSTRAINT `fk_re_ledger` FOREIGN KEY (`id_points_ledger`) REFERENCES `points_ledger` (`id_points_ledger`),
  CONSTRAINT `fk_re_mmv` FOREIGN KEY (`id_modifiable_mechanic_videogame`) REFERENCES `modifiable_mechanic_videogames` (`id_modifiable_mechanic_videogame`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `redemption_event`
--

LOCK TABLES `redemption_event` WRITE;
/*!40000 ALTER TABLE `redemption_event` DISABLE KEYS */;
INSERT INTO `redemption_event` VALUES (1,3,1,30,'2025-12-10 19:06:00','{\"note\": \"Primer canje jmacias en UpperWish\"}'),(2,6,1,50,'2025-12-09 21:01:00','{\"note\": \"Primer canje hherrera en UpperWish\"}');
/*!40000 ALTER TABLE `redemption_event` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_redemption_event_del` AFTER DELETE ON `redemption_event` FOR EACH ROW BEGIN
  INSERT INTO audit_log (table_name, op, row_pk, changed_by, old_row)
  VALUES (
    'redemption_event',
    'DELETE',
    CAST(OLD.id_redemption_event AS CHAR),
    COALESCE(@app_user, CURRENT_USER()),
    JSON_OBJECT(
      'id_redemption_event',          OLD.id_redemption_event,
      'id_points_ledger',            OLD.id_points_ledger,
      'id_modifiable_mechanic_videogame', OLD.id_modifiable_mechanic_videogame,
      'redeemed_points',             OLD.redeemed_points,
      'redeemed_at',                 OLD.redeemed_at,
      'metadata',                    OLD.metadata
    )
  );
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `sensor_endpoint`
--

DROP TABLE IF EXISTS `sensor_endpoint`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `sensor_endpoint` (
  `id_sensor_endpoint` int NOT NULL AUTO_INCREMENT,
  `sensor_endpoint_id_online_sensor` int NOT NULL,
  `name` varchar(100) DEFAULT NULL,
  `description` varchar(1000) DEFAULT NULL,
  `url_endpoint` varchar(1000) DEFAULT NULL,
  `token_parameters` json DEFAULT NULL,
  `specific_parameters` json DEFAULT NULL,
  `watch_parameters` json DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id_sensor_endpoint`),
  KEY `fk_se_online` (`sensor_endpoint_id_online_sensor`),
  CONSTRAINT `fk_se_online` FOREIGN KEY (`sensor_endpoint_id_online_sensor`) REFERENCES `online_sensor` (`id_online_sensor`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sensor_endpoint`
--

LOCK TABLES `sensor_endpoint` WRITE;
/*!40000 ALTER TABLE `sensor_endpoint` DISABLE KEYS */;
INSERT INTO `sensor_endpoint` VALUES (1,1,'Daily Steps','Pasos diarios desde Fitbit Demo','/v1/steps','{\"grant_type\": \"bearer\"}','{\"granularity\": \"day\"}','{\"field\": \"steps\"}','2025-12-11 21:23:46','2025-12-11 21:23:46'),(2,2,'Nightly Sleep','Horas de sueño desde SleepTracker Demo','/v1/sleep','{\"grant_type\": \"bearer\"}','{\"granularity\": \"night\"}','{\"field\": \"minutes_asleep\"}','2025-12-11 21:23:46','2025-12-11 21:23:46');
/*!40000 ALTER TABLE `sensor_endpoint` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sensor_ingest_event`
--

DROP TABLE IF EXISTS `sensor_ingest_event`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `sensor_ingest_event` (
  `id_sensor_ingest_event` bigint NOT NULL AUTO_INCREMENT,
  `id_players` int NOT NULL,
  `id_players_sensor_endpoint` int DEFAULT NULL,
  `id_sensor_endpoint` int DEFAULT NULL,
  `raw_payload` json NOT NULL,
  `parsed_value` decimal(18,6) DEFAULT NULL,
  `status` enum('OK','ERROR','IGNORED') NOT NULL DEFAULT 'OK',
  `error_message` varchar(512) DEFAULT NULL,
  `occurred_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id_sensor_ingest_event`),
  KEY `ix_sie_time` (`occurred_at`),
  KEY `ix_sie_player_time` (`id_players`,`occurred_at`),
  KEY `fk_sie_pse` (`id_players_sensor_endpoint`),
  KEY `fk_sie_se` (`id_sensor_endpoint`),
  CONSTRAINT `fk_sie_player` FOREIGN KEY (`id_players`) REFERENCES `players` (`id_players`),
  CONSTRAINT `fk_sie_pse` FOREIGN KEY (`id_players_sensor_endpoint`) REFERENCES `players_sensor_endpoint` (`id_players_sensor_endpoint`),
  CONSTRAINT `fk_sie_se` FOREIGN KEY (`id_sensor_endpoint`) REFERENCES `sensor_endpoint` (`id_sensor_endpoint`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sensor_ingest_event`
--

LOCK TABLES `sensor_ingest_event` WRITE;
/*!40000 ALTER TABLE `sensor_ingest_event` DISABLE KEYS */;
INSERT INTO `sensor_ingest_event` VALUES (1,26,1,1,'{\"date\": \"2025-12-10\", \"steps\": 8000}',8000.000000,'OK',NULL,'2025-12-10 07:30:00','2025-12-10 07:30:05'),(2,26,2,2,'{\"date\": \"2025-12-10\", \"minutes_asleep\": 420}',420.000000,'OK',NULL,'2025-12-10 23:30:00','2025-12-10 23:30:05'),(3,22,3,1,'{\"date\": \"2025-12-09\", \"steps\": 6000}',6000.000000,'OK',NULL,'2025-12-09 08:00:00','2025-12-09 08:00:05'),(4,24,5,1,'{\"date\": \"2025-12-09\", \"steps\": 10000}',10000.000000,'OK',NULL,'2025-12-09 18:30:00','2025-12-09 18:30:05');
/*!40000 ALTER TABLE `sensor_ingest_event` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_sensor_ingest_event_del` AFTER DELETE ON `sensor_ingest_event` FOR EACH ROW BEGIN
  INSERT INTO audit_log (table_name, op, row_pk, changed_by, old_row)
  VALUES (
    'sensor_ingest_event',
    'DELETE',
    CAST(OLD.id_sensor_ingest_event AS CHAR),
    COALESCE(@app_user, CURRENT_USER()),
    JSON_OBJECT(
      'id_sensor_ingest_event',    OLD.id_sensor_ingest_event,
      'id_players',                OLD.id_players,
      'id_players_sensor_endpoint',OLD.id_players_sensor_endpoint,
      'id_sensor_endpoint',        OLD.id_sensor_endpoint,
      'raw_payload',               OLD.raw_payload,
      'parsed_value',              OLD.parsed_value,
      'status',                    OLD.status,
      'error_message',             OLD.error_message,
      'occurred_at',               OLD.occurred_at,
      'created_at',                OLD.created_at
    )
  );
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `subattributes`
--

DROP TABLE IF EXISTS `subattributes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `subattributes` (
  `id_subattributes` int NOT NULL AUTO_INCREMENT,
  `name` varchar(45) DEFAULT NULL,
  `description` varchar(300) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `attributes_id_attributes` int NOT NULL,
  PRIMARY KEY (`id_subattributes`),
  UNIQUE KEY `uq_sub_name_per_attr` (`attributes_id_attributes`,`name`),
  KEY `ix_subattr_attr` (`attributes_id_attributes`),
  CONSTRAINT `fk_subattr_attr` FOREIGN KEY (`attributes_id_attributes`) REFERENCES `attributes` (`id_attributes`)
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `subattributes`
--

LOCK TABLES `subattributes` WRITE;
/*!40000 ALTER TABLE `subattributes` DISABLE KEYS */;
INSERT INTO `subattributes` VALUES (1,'Habilidades de comunicación interpersonal','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',1),(2,'Red de apoyo social','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',1),(3,'Resolución de conflictos','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',1),(4,'Participación comunitaria','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',1),(5,'Empatía y habilidades emocionales','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',1),(6,'Condición física','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',2),(7,'Desarrollo motor grueso y fino','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',2),(8,'Coordinación viso-motora','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',2),(9,'Nutrición y metabolismo','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',2),(10,'Salud física general','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',2),(11,'Regulación emocional','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',3),(12,'Reconocimiento de emociones propias y ajenas','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',3),(13,'Manejo de estrés','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',3),(14,'Relación afectiva con otros','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',3),(15,'Expresión emocional','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',3),(16,'Procesos de memoria y aprendizaje','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',4),(17,'Razonamiento lógico y analítico','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',4),(18,'Toma de decisiones','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',4),(19,'Pensamiento creativo','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',4),(20,'Resolución de problemas','placeholder','2025-09-26 03:14:07','2025-09-26 03:14:07',4);
/*!40000 ALTER TABLE `subattributes` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_subattributes_ins` AFTER INSERT ON `subattributes` FOR EACH ROW BEGIN
  INSERT INTO audit_log(table_name, op, row_pk, changed_by, new_row)
  VALUES ('subattributes','INSERT', CAST(NEW.id_subattributes AS CHAR),
          COALESCE(@app_user, CURRENT_USER()),
          JSON_OBJECT('id_subattributes',NEW.id_subattributes,'name',NEW.name,
                      'description',NEW.description,'attributes_id_attributes',NEW.attributes_id_attributes,
                      'created_at',NEW.created_at,'updated_at',NEW.updated_at));
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_subattributes_upd` AFTER UPDATE ON `subattributes` FOR EACH ROW BEGIN
  INSERT INTO audit_log(table_name, op, row_pk, changed_by, old_row, new_row)
  VALUES ('subattributes','UPDATE', CAST(NEW.id_subattributes AS CHAR),
          COALESCE(@app_user, CURRENT_USER()),
          JSON_OBJECT('id_subattributes',OLD.id_subattributes,'name',OLD.name,
                      'description',OLD.description,'attributes_id_attributes',OLD.attributes_id_attributes,
                      'created_at',OLD.created_at,'updated_at',OLD.updated_at),
          JSON_OBJECT('id_subattributes',NEW.id_subattributes,'name',NEW.name,
                      'description',NEW.description,'attributes_id_attributes',NEW.attributes_id_attributes,
                      'created_at',NEW.created_at,'updated_at',NEW.updated_at));
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`%`*/ /*!50003 TRIGGER `trg_subattributes_del` AFTER DELETE ON `subattributes` FOR EACH ROW BEGIN
  INSERT INTO audit_log(table_name, op, row_pk, changed_by, old_row)
  VALUES ('subattributes','DELETE', CAST(OLD.id_subattributes AS CHAR),
          COALESCE(@app_user, CURRENT_USER()),
          JSON_OBJECT('id_subattributes',OLD.id_subattributes,'name',OLD.name,
                      'description',OLD.description,'attributes_id_attributes',OLD.attributes_id_attributes,
                      'created_at',OLD.created_at,'updated_at',OLD.updated_at));
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `subattributes_conversion_sensor_endpoint`
--

DROP TABLE IF EXISTS `subattributes_conversion_sensor_endpoint`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `subattributes_conversion_sensor_endpoint` (
  `id_subattributes_conversion_sensor_endpoint` int NOT NULL AUTO_INCREMENT,
  `id_subattributes` int NOT NULL,
  `id_sensor_endpoint` int NOT NULL,
  `id_conversion` int NOT NULL,
  `parameters_watched` json DEFAULT NULL,
  PRIMARY KEY (`id_subattributes_conversion_sensor_endpoint`),
  KEY `fk_scse_sub` (`id_subattributes`),
  KEY `fk_scse_se` (`id_sensor_endpoint`),
  KEY `fk_scse_conv` (`id_conversion`),
  CONSTRAINT `fk_scse_conv` FOREIGN KEY (`id_conversion`) REFERENCES `conversion` (`id_conversion`),
  CONSTRAINT `fk_scse_se` FOREIGN KEY (`id_sensor_endpoint`) REFERENCES `sensor_endpoint` (`id_sensor_endpoint`),
  CONSTRAINT `fk_scse_sub` FOREIGN KEY (`id_subattributes`) REFERENCES `subattributes` (`id_subattributes`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `subattributes_conversion_sensor_endpoint`
--

LOCK TABLES `subattributes_conversion_sensor_endpoint` WRITE;
/*!40000 ALTER TABLE `subattributes_conversion_sensor_endpoint` DISABLE KEYS */;
INSERT INTO `subattributes_conversion_sensor_endpoint` VALUES (1,6,1,1,'{\"unit\": \"count\", \"metric\": \"steps\"}'),(2,11,2,1,'{\"unit\": \"min\", \"metric\": \"minutes_asleep\"}');
/*!40000 ALTER TABLE `subattributes_conversion_sensor_endpoint` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary view structure for view `v_game_usage_points`
--

DROP TABLE IF EXISTS `v_game_usage_points`;
/*!50001 DROP VIEW IF EXISTS `v_game_usage_points`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `v_game_usage_points` AS SELECT
 1 AS `id_players`,
 1 AS `id_videogame`,
 1 AS `points_spent`,
 1 AS `seconds_with_lsg`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary view structure for view `v_player_attribute_balance`
--

DROP TABLE IF EXISTS `v_player_attribute_balance`;
/*!50001 DROP VIEW IF EXISTS `v_player_attribute_balance`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `v_player_attribute_balance` AS SELECT
 1 AS `id_players`,
 1 AS `player_name`,
 1 AS `player_email`,
 1 AS `id_attributes`,
 1 AS `attribute_name`,
 1 AS `balance_ledger`,
 1 AS `snapshot_points`,
 1 AS `diff_ledger_minus_snapshot`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary view structure for view `v_player_game_overview`
--

DROP TABLE IF EXISTS `v_player_game_overview`;
/*!50001 DROP VIEW IF EXISTS `v_player_game_overview`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `v_player_game_overview` AS SELECT
 1 AS `id_players`,
 1 AS `player_name`,
 1 AS `player_email`,
 1 AS `id_videogame`,
 1 AS `videogame_name`,
 1 AS `points_spent`,
 1 AS `seconds_with_lsg`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary view structure for view `v_points_balance`
--

DROP TABLE IF EXISTS `v_points_balance`;
/*!50001 DROP VIEW IF EXISTS `v_points_balance`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `v_points_balance` AS SELECT
 1 AS `id_players`,
 1 AS `id_point_dimension`,
 1 AS `balance`*/;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `videogame`
--

DROP TABLE IF EXISTS `videogame`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `videogame` (
  `id_videogame` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `genre` varchar(45) DEFAULT NULL,
  `engine` varchar(45) DEFAULT NULL,
  `developer` varchar(128) DEFAULT NULL,
  `publisher` varchar(128) DEFAULT NULL,
  `launch` varchar(45) DEFAULT NULL,
  `version` varchar(128) DEFAULT NULL,
  `type` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id_videogame`),
  KEY `ix_videogame_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=20 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `videogame`
--

LOCK TABLES `videogame` WRITE;
/*!40000 ALTER TABLE `videogame` DISABLE KEYS */;
INSERT INTO `videogame` VALUES (1,'UpperWish','RPG-TBS','none','Estudiantes USACH','none','2022','1.0','GFS (Game From Scratch)'),(2,'Spirit Adventure','RPG','none','Ricardo Ruz','none','2022','1.0','GFS (Game From Scratch)'),(3,'Village Defender','RTS','none','Gustavo Ternero','none','2022','1.0','GFS (Game From Scratch)'),(4,'Street Blocks','AVG','none','Eduardo Lizama','none','2022','1.0','GFS (Game From Scratch)'),(5,'Blazing Duel','AVG','none','Bastían Onetto','none','2023','1.0','GFS (Game From Scratch)'),(6,'ZonaCero','FPS','none','Ignacio Fernández','none','2023','1.0','GFS (Game From Scratch)'),(7,'Minecraft','RPG','none','Gary Simken','none','2023','1.0','MOD'),(8,'Terraria','RPG','none','Claudio Muñoz','none','2024','1.0','MOD'),(9,'WealthQuest','TG','none','Jonathan Soto','none','2024','1.0','GFS (Game From Scratch)'),(10,'Nightmare Survivor','AVG','none','Jeison Fiorentino','none','2024','1.0','GFS (Game From Scratch)'),(11,'Digital Masters','SGS','none','Vicente Vargas','none','2024','1.0','GFS (Game From Scratch)'),(12,'Stardew Valley','RPG','none','Moisés Godoy','none','2024','1.0','MOD'),(13,'Bulletland','AVG','none','Gianfranco Piccinini','none','2024','1.0','GFS (Game From Scratch)'),(14,'Cities: Skylines','Simulation','none','Alejandro Aldea','none','In progress (-2025)','none','MOD'),(15,'Starbound','RPG','none','Hernan Herrera','none','In progress (-2025)','none','MOD'),(16,'Corekeeper','RPG','none','Luis Mellado','none','In progress (-2026)','none','MOD'),(17,'Valheim','PRG','none','Nicolas Gabrielli','none','In progress (-2026)','none','MOD'),(18,'Ark: Survival Evolved','RPG','none','William Jimenez','none','In progress (-2026)','none','MOD'),(19,'Subnautica: Below Zero','RPG','none','Ricardo Avaca','none','In progress (-2026)','none','MOD');
/*!40000 ALTER TABLE `videogame` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping events for database 'db_lsg'
--

--
-- Dumping routines for database 'db_lsg'
--
/*!50003 DROP FUNCTION IF EXISTS `sp_get_att_subattributes_id` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` FUNCTION `sp_get_att_subattributes_id`() RETURNS json
    READS SQL DATA
    DETERMINISTIC
BEGIN
  RETURN (
    SELECT JSON_ARRAYAGG(
             JSON_OBJECT(
               'attribute', id_attributes,
               'subattribute', sub.id_subattributes
             )
           )
    FROM attributes att
    JOIN subattributes sub
      ON att.id_attributes = sub.attributes_id_attributes
    ORDER BY att.id_attributes, sub.id_subattributes
  );
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `sp_get_att_subattributes_name` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` FUNCTION `sp_get_att_subattributes_name`() RETURNS json
    READS SQL DATA
    DETERMINISTIC
BEGIN
  RETURN (
    SELECT JSON_ARRAYAGG(
             JSON_OBJECT(
               'attribute', att.name,
               'subattribute', sub.name
             )
           )
    FROM attributes att
    JOIN subattributes sub
      ON att.id_attributes = sub.attributes_id_attributes
    ORDER BY att.id_attributes, sub.id_subattributes
  );
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `sp_get_players_att_points` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` FUNCTION `sp_get_players_att_points`() RETURNS json
    READS SQL DATA
    DETERMINISTIC
BEGIN
  RETURN (
    SELECT JSON_ARRAYAGG(
             JSON_OBJECT(
               'name_players', pla.name,
               'email_players', pla.email,
               'attributes', att.name,
               'points', plaatt.data
             )
           )
    FROM players pla
    JOIN players_attributes plaatt
      ON pla.id_players = plaatt.id_players
    JOIN attributes att
      ON plaatt.id_attributes = att.id_attributes
    ORDER BY pla.name, pla.email
  );
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `sp_get_videogame` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` FUNCTION `sp_get_videogame`() RETURNS json
    READS SQL DATA
    DETERMINISTIC
BEGIN
  RETURN (
    SELECT JSON_ARRAYAGG(
             JSON_OBJECT(
               'name', vid.name,
               'developer', vid.developer,
               'launch', vid.launch,
               'genre', vid.genre,
               'type', vid.type,
               'version', vid.version
             )
           )
    FROM videogame vid
    ORDER BY vid.name, vid.developer
  );
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `sp_delete_player_cascade` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `sp_delete_player_cascade`(IN p_id INT)
BEGIN
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Error al borrar en cascada al player';
  END;

  START TRANSACTION;

  /* 1) Eventos de canje que dependen del ledger */
  DELETE re
    FROM redemption_event re
    JOIN points_ledger pl ON pl.id_points_ledger = re.id_points_ledger
   WHERE pl.id_players = p_id;

  /* 2) Ledger de puntos */
  DELETE FROM points_ledger WHERE id_players = p_id;

  /* 3) Ingestas de sensores (referencia directa a players y opcional a PSE/SE) */
  DELETE FROM sensor_ingest_event WHERE id_players = p_id;

  /* 4) Sesiones LSG (hija de player_videogame) */
  DELETE s
    FROM lsg_game_session s
    JOIN player_videogame pvg ON pvg.id_player_videogame = s.id_player_videogame
   WHERE pvg.id_players = p_id;

  /* 5) Relación jugador–videojuego */
  DELETE FROM player_videogame WHERE id_players = p_id;

  /* 6) Vínculos con sensores (online y endpoints) */
  DELETE FROM player_online_sensor    WHERE id_players = p_id;
  DELETE FROM players_sensor_endpoint WHERE id_players = p_id;

  /* 7) Tablas legacy de acumulados/gastos por atributo */
  DELETE FROM adquired_subattribute   WHERE id_players = p_id;
  DELETE FROM expended_attribute      WHERE id_players = p_id;

  /* 8) Snapshot/caché de atributos */
  DELETE FROM players_attributes      WHERE id_players = p_id;

  /* 9) Finalmente, el jugador */
  DELETE FROM players                 WHERE id_players = p_id;

  COMMIT;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `sp_init_all_players_attributes` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `sp_init_all_players_attributes`()
    MODIFIES SQL DATA
BEGIN
  INSERT INTO players_attributes (id_players, id_attributes, data)
  SELECT
    p.id_players,
    a.id_attributes,
    0 AS data
  FROM players p
  CROSS JOIN attributes a
  LEFT JOIN players_attributes pa
    ON pa.id_players   = p.id_players
   AND pa.id_attributes = a.id_attributes
  WHERE pa.id_players IS NULL;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `sp_init_player_attributes` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `sp_init_player_attributes`(IN p_id_players INT)
    MODIFIES SQL DATA
BEGIN
  /* Inserta filas en players_attributes para todos los attributes
     que aún no existen para este jugador, con data = 0 */
  INSERT INTO players_attributes (id_players, id_attributes, data)
  SELECT
      p_id_players        AS id_players,
      a.id_attributes     AS id_attributes,
      0                   AS data
  FROM attributes a
  LEFT JOIN players_attributes pa
    ON pa.id_players   = p_id_players
   AND pa.id_attributes = a.id_attributes
  WHERE pa.id_players IS NULL;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Final view structure for view `v_game_usage_points`
--

/*!50001 DROP VIEW IF EXISTS `v_game_usage_points`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `v_game_usage_points` AS select `pvg`.`id_players` AS `id_players`,`pvg`.`id_videogame` AS `id_videogame`,sum((case when (`pl`.`direction` = 'DEBIT') then `pl`.`amount` else 0 end)) AS `points_spent`,sum(`s`.`duration_seconds`) AS `seconds_with_lsg` from ((`player_videogame` `pvg` left join `lsg_game_session` `s` on((`s`.`id_player_videogame` = `pvg`.`id_player_videogame`))) left join `points_ledger` `pl` on(((`pl`.`id_players` = `pvg`.`id_players`) and (`pl`.`id_videogame` = `pvg`.`id_videogame`)))) group by `pvg`.`id_players`,`pvg`.`id_videogame` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `v_player_attribute_balance`
--

/*!50001 DROP VIEW IF EXISTS `v_player_attribute_balance`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `v_player_attribute_balance` AS select `p`.`id_players` AS `id_players`,`p`.`name` AS `player_name`,`p`.`email` AS `player_email`,`a`.`id_attributes` AS `id_attributes`,`a`.`name` AS `attribute_name`,coalesce(sum(`vpb`.`balance`),0) AS `balance_ledger`,coalesce(`pa`.`data`,0) AS `snapshot_points`,(coalesce(sum(`vpb`.`balance`),0) - coalesce(`pa`.`data`,0)) AS `diff_ledger_minus_snapshot` from ((((`players` `p` join `attributes` `a`) left join `players_attributes` `pa` on(((`pa`.`id_players` = `p`.`id_players`) and (`pa`.`id_attributes` = `a`.`id_attributes`)))) left join (select `pd`.`id_point_dimension` AS `id_point_dimension`,coalesce(`pd`.`id_attributes`,`s`.`attributes_id_attributes`) AS `id_attributes` from (`point_dimension` `pd` left join `subattributes` `s` on((`s`.`id_subattributes` = `pd`.`id_subattributes`)))) `pd_map` on((`pd_map`.`id_attributes` = `a`.`id_attributes`))) left join `v_points_balance` `vpb` on(((`vpb`.`id_players` = `p`.`id_players`) and (`vpb`.`id_point_dimension` = `pd_map`.`id_point_dimension`)))) group by `p`.`id_players`,`p`.`name`,`p`.`email`,`a`.`id_attributes`,`a`.`name`,`pa`.`data` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `v_player_game_overview`
--

/*!50001 DROP VIEW IF EXISTS `v_player_game_overview`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `v_player_game_overview` AS select `p`.`id_players` AS `id_players`,`p`.`name` AS `player_name`,`p`.`email` AS `player_email`,`vg`.`id_videogame` AS `id_videogame`,`vg`.`name` AS `videogame_name`,coalesce(`gup`.`points_spent`,0) AS `points_spent`,coalesce(`gup`.`seconds_with_lsg`,0) AS `seconds_with_lsg` from (((`players` `p` join `player_videogame` `pvg` on((`pvg`.`id_players` = `p`.`id_players`))) join `videogame` `vg` on((`vg`.`id_videogame` = `pvg`.`id_videogame`))) left join `v_game_usage_points` `gup` on(((`gup`.`id_players` = `p`.`id_players`) and (`gup`.`id_videogame` = `vg`.`id_videogame`)))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `v_points_balance`
--

/*!50001 DROP VIEW IF EXISTS `v_points_balance`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `v_points_balance` AS select `points_ledger`.`id_players` AS `id_players`,`points_ledger`.`id_point_dimension` AS `id_point_dimension`,sum((case when (`points_ledger`.`direction` = 'CREDIT') then `points_ledger`.`amount` else -(`points_ledger`.`amount`) end)) AS `balance` from `points_ledger` group by `points_ledger`.`id_players`,`points_ledger`.`id_point_dimension` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-12-11 18:36:38
