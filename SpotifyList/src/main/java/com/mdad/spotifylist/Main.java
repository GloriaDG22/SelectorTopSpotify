/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mdad.spotifylist;

import com.mdad.windows.Interface;

/**
 *
 * @author gloria
 */
public class Main {

    public static void main(String[] args) {
        try {
            
            Interface inter = new Interface();
            inter.setVisible(true);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
