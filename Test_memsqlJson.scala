// Databricks notebook source
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;


val mapper = new ObjectMapper();

val jsonData = "{\"name\":\"mkyong\", \"age\":\"37\"}";

Map<String, String> map = mapper.readValue(json, Map.class);

val map1 = JSON.parseFull(jsonData).get.asInstanceOf[Map[String,Any]]
