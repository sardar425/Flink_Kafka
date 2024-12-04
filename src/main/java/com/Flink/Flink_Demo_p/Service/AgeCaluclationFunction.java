package com.Flink.Flink_Demo_p.Service;

import com.Flink.Flink_Demo_p.Dto.UserEntity;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class AgeCaluclationFunction implements MapFunction<UserEntity, UserEntity> {
    @Override
    public UserEntity map(UserEntity user) throws Exception{
        int age = calculateAge(user.getDob());
        user.setAge(age);
        return user;
    }

    private int calculateAge(String dateofBirth){
        DateTimeFormatter dataformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate birthdate = LocalDate.parse(dateofBirth, dataformatter);
        LocalDate currentdate = LocalDate.now();
        int age =currentdate.getYear() - birthdate.getYear();

        if (currentdate.getMonthValue() < birthdate.getMonthValue() ||
                (currentdate.getMonthValue() == birthdate.getMonthValue() && currentdate.getMonthValue() <
                        birthdate.getMonthValue())){
            age--;
        }
        return age;
    }
}
