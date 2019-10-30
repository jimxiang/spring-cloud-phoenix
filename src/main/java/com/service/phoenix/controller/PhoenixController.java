package com.service.phoenix.controller;

import com.service.phoenix.entity.MessageResponse;
import com.service.phoenix.service.PhoenixService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/phoenix")
public class PhoenixController {

    @Autowired
    private PhoenixService phoenixService;

    @RequestMapping(value = "/phone_list")
    public MessageResponse getContactListByUserId(@RequestParam String user_id,
                                                  @RequestParam(defaultValue = "20") int limit,
                                                  @RequestParam(defaultValue = "0") int offset,
                                                  @RequestParam(defaultValue = "") String upload_time) {
        return phoenixService.getContactListByUserId(user_id, limit, offset, upload_time);
    }

    @RequestMapping(value = "/update_register_user")
    public MessageResponse updateRegisterStatus(@RequestParam String phone,
                                                @RequestParam String phone_uid,
                                                @RequestParam String register_time) {
        return phoenixService.updateRegisterStatus(phone, phone_uid, register_time);
    }

    @RequestMapping(value = "/get_register_user")
    public MessageResponse getRegisterUser(@RequestParam String user_id,
                                           @RequestParam(defaultValue = "") String register_time,
                                           @RequestParam(defaultValue = "20") int limit,
                                           @RequestParam(defaultValue = "0") int offset) {
        return phoenixService.getRegisterUser(user_id, register_time, limit, offset);
    }

    @RequestMapping(value = "/get_first_degree_register")
    public MessageResponse getFirstDegreeRegister(@RequestParam String user_id,
                                                  @RequestParam(defaultValue = "20") int limit,
                                                  @RequestParam(defaultValue = "0") int offset) {
        return phoenixService.getFirstDegreeRegister(user_id, limit, offset);
    }

    @RequestMapping(value = "/get_company_job_list", method = RequestMethod.POST)
    public MessageResponse getCompanyJobList(@RequestBody List<String> eidList) {
        return phoenixService.getCompanyJobList(eidList);
    }

    @RequestMapping(value = "/get_similar_company", method = RequestMethod.POST)
    public MessageResponse getSimilarCompany(@RequestBody List<String> eidList) {
        return phoenixService.getSimilarCompany(eidList);
    }

}
