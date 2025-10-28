package com.eyelevel.documentprocessor.service.zip;

import com.eyelevel.documentprocessor.dto.metric.ZipStatusFileNameDto;
import com.eyelevel.documentprocessor.model.ZipProcessingStatus;
import com.eyelevel.documentprocessor.repository.ZipMasterRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class ZipProcessingListService {
    private final ZipMasterRepository zipMasterRepository;

    public Map<ZipProcessingStatus, List<String>> listZipFilesByProcessingStatus() {
        Map<ZipProcessingStatus, List<String>> result = new EnumMap<>(ZipProcessingStatus.class);

        List<ZipStatusFileNameDto> zipFiles = zipMasterRepository.findStatusAndFileNameByStatusIn(
                List.of(ZipProcessingStatus.EXTRACTION_IN_PROGRESS, ZipProcessingStatus.QUEUED_FOR_EXTRACTION)
        );

        zipFiles.forEach(zip ->
                result.computeIfAbsent(zip.getStatus(), k -> new ArrayList<>()).add(zip.getFilename())
        );

        return result;
    }


}
