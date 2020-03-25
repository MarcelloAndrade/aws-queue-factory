package com.queuefactory.examples;

import java.io.Serializable;

import lombok.Data;

@Data
public class User implements Serializable {
	
	private static final long serialVersionUID = 1442258538672827765L;
	
	private String name;
	private int age;

}
