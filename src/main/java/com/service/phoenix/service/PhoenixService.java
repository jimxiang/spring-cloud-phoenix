package com.service.phoenix.service;

import com.service.phoenix.entity.MessageResponse;

import java.util.List;

public interface PhoenixService {

    MessageResponse getContactListByUserId(String userId, int limit, int offset, String uploadTime);

    MessageResponse updateRegisterStatus(String phone, String phoneUid, String registerTime);

    MessageResponse getRegisterUser(String userId, String registerTime, int limit, int offset);

    MessageResponse getFirstDegreeRegister(String userId, int limit, int offset);

    MessageResponse getCompanyJobList(List<String> eidList);

    MessageResponse getSimilarCompany(List<String> eidList);
}
