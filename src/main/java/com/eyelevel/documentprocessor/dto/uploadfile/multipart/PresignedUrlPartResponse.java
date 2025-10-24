package com.eyelevel.documentprocessor.dto.uploadfile.multipart;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.net.URL;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class PresignedUrlPartResponse {
    private URL presignedUrl;
}
