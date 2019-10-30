package com.service.phoenix.service;

import com.service.phoenix.entity.MessageResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service(value = "phoenixService")
public class PhoenixServiceImpl implements PhoenixService {

    @Autowired
    private DataSourceService dataSourceService;

    @Override
    public MessageResponse getContactListByUserId(String userId, int limit, int offset, String uploadTime) {
        return dataSourceService.queryContactListByUserId(userId, limit, offset, uploadTime);
    }

    @Override
    public MessageResponse updateRegisterStatus(String phone, String phoneUid, String registerTime) {
        return dataSourceService.updateRegisterStatus(phone, phoneUid, registerTime);
    }

    @Override
    public MessageResponse getRegisterUser(String userId, String registerTime, int limit, int offset) {
        return dataSourceService.getRegisterUser(userId, registerTime, limit, offset);
    }

    @Override
    public MessageResponse getFirstDegreeRegister(String userId, int limit, int offset) {
        return dataSourceService.getFirstDegreeRegister(userId, limit, offset);
    }

    @Override
    public MessageResponse getCompanyJobList(List<String> eidList) {
        return dataSourceService.getCompanyJobList(eidList);
    }

    @Override
    public MessageResponse getSimilarCompany(List<String> eidList) {
        return dataSourceService.getSimilarCompany(eidList);
    }
}
