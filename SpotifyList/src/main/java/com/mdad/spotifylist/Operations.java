/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mdad.spotifylist;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.Iterator;

/**
 *
 * @author gloria
 */
public class Operations {
    
    DBConnector connector;
    
    private int row=0;
    
    public Operations(){
        connector = new DBConnector();
        connector.connect("localhost", 9042);
        
    }
    
    public void createKeySpace(){
        final String createmovieKeySpace = "CREATE KEYSPACE IF NOT EXISTS songs_keyspace WITH "
                    + "replication = {'class' : 'SimpleStrategy','replication_factor' :3}";

        connector.getSession().execute(createmovieKeySpace);
        
        System.out.println("Se ha creado songs_keyspace");
            
    }
    
    public void createTable(){
        final String createmovieTable = "CREATE TABLE IF NOT EXISTS songs_keyspace.songs"
                    + "(id varchar PRIMARY KEY, title varchar , artist varchar, topGenre varchar, year varchar, bpm varchar, nrgy varchar, dnce varchar, dB varchar, live varchar, val varchar, dur varchar, acous varchar, spch varchar, pop varchar)";

        connector.getSession().execute(createmovieTable);
        
        final String selectQuery = "SELECT * FROM songs_keyspace.songs";
        
        ResultSet rs = connector.getSession().execute(selectQuery);
        
        row = rs.all().size();
        
        System.out.println("Se ha creado la tabla songs");
    }
    
    public void deleteTable(){
        final String createmovieTable = "DROP TABLE songs_keyspace.songs";

        connector.getSession().execute(createmovieTable);
        
        
        System.out.println("Se ha eliminado la tabla songs");
    }
 
    public void insertData(String[] data){
        row += 1;
        final String insertQuery = "INSERT INTO songs_keyspace.songs (id, title, artist, topGenre, year) VALUES ('" + row + "',?,?,?,?)";

        PreparedStatement psInsert = connector.getSession().prepare(insertQuery);
        BoundStatement bsInsert = psInsert.bind(data[0], data[1], data[2], data[3]);
        connector.getSession().execute(bsInsert);
    }
    
    public Object[][] searchData(String att, String val){
        final String selectQuery;
        ResultSet rs;
        Object[][] data;
        
        
            
        if (val.equals(" ") && !att.equals("all")){
            
            selectQuery = "SELECT \""+ att + "\" FROM songs_keyspace.songs";
            rs = connector.getSession().execute(selectQuery);
            
            int col = 1;
            data = new Object[row][col];
            
            int i = 0;
            
            for (Iterator <Row> iterator = rs.iterator(); rs.iterator().hasNext();) {
                Row rr = iterator.next();
                
                System.out.println("song " + att + " : " + rr.getString(att));
                data[i][0]= rr.getString(att);
                i++;
            }
        }else{
            if(att.equals("all")){
                selectQuery = "SELECT * FROM songs_keyspace.songs";
        
                rs = connector.getSession().execute(selectQuery);
        
            }else {
                selectQuery = "SELECT * FROM songs_keyspace.songs WHERE " + att + "=? ALLOW FILTERING";

                PreparedStatement psSelect = connector.getSession().prepare(selectQuery);
                BoundStatement bsSelect = psSelect.bind(val);

                rs = connector.getSession().execute(bsSelect);
            }    
            
            int col = 15;
            data = new Object[row][col];
            
            int i = 0;
            for (Iterator <Row> iterator = rs.iterator(); rs.iterator().hasNext();) {
                Row rr = iterator.next();
                
                data[i][0]= rr.getString("id");
                data[i][1]= rr.getString("title");
                data[i][2]= rr.getString("artist");
                data[i][3]= rr.getString("topGenre"); 
                data[i][4]= rr.getString("year");
                data[i][5]= rr.getString("bpm");
                data[i][6]= rr.getString("nrgy");
                data[i][7]= rr.getString("dnce");
                data[i][8]= rr.getString("dB");
                data[i][9]= rr.getString("live");
                data[i][10]= rr.getString("val");
                data[i][11]= rr.getString("dur");
                data[i][12]= rr.getString("acous");
                data[i][13]= rr.getString("spch");
                data[i][14]= rr.getString("pop");
            
                i++;
            }
        }
            
      
        return data;
    }
    
    public void updateData(String att, String val, String [] data){        
        
        final String selectQuery = "SELECT id FROM songs_keyspace.songs WHERE "+ att+"=? ALLOW FILTERING";
        PreparedStatement psSelect = connector.getSession().prepare(selectQuery);
        BoundStatement bsSelect = psSelect.bind(val);
        ResultSet rs = connector.getSession().execute(bsSelect);
            
        Row rr = rs.iterator().next();
        
        final String updateQuery = "UPDATE songs_keyspace.songs SET title= ?, artist=?, topGenre=?, year = ? WHERE id=?";

        PreparedStatement psUpdate = connector.getSession().prepare(updateQuery);
        BoundStatement bsUpdate = psUpdate.bind(data[0], data[1], data[2], data[3], rr.getString("id"));
        connector.getSession().execute(bsUpdate);
    }
    
    public void deleteData(String att, String val){
        final String selectQuery = "SELECT id FROM songs_keyspace.songs WHERE "+ att+"=? ALLOW FILTERING";
        PreparedStatement psSelect = connector.getSession().prepare(selectQuery);
        BoundStatement bsSelect = psSelect.bind(val);
        ResultSet rs = connector.getSession().execute(bsSelect);
            
        Row rr = rs.iterator().next();
        
        final String deleteQuery = "DELETE FROM songs_keyspace.songs WHERE id= ?";

        PreparedStatement psDelete = connector.getSession().prepare(deleteQuery);
        BoundStatement bsDelete = psDelete.bind(rr.getString("id"));
        connector.getSession().execute(bsDelete);
    }
    
    public void cerrar(){
        connector.close();
    }
}
