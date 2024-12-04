package com.Flink.Flink_Demo_p.Dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserEntity {
    private int age;
    private String name;
    private String address;
    private String dob;

}
