package com.ontology.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service("ConfigParam")
public class ConfigParam {

	/**
	 *  SDK参数
	 */
	@Value("${service.restfulUrl}")
	public String RESTFUL_URL;

	@Value("${service.payer.address}")
	public String PAYER_ADDRESS;

	@Value("${contract.hash}")
	public String CONTRACT_HASH;

	@Value("${contract.hash.mp.auth}")
	public String CONTRACT_HASH_MP_AUTH;

	@Value("${contract.hash.mp}")
	public String CONTRACT_HASH_MP;

	@Value("${contract.hash.dtoken}")
	public String CONTRACT_HASH_DTOKEN;

}