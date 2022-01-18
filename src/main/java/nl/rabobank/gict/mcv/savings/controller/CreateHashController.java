package nl.rabobank.gict.mcv.savings.controller;

import nl.rabobank.gict.mcv.savings.model.Hash;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping
public class CreateHashController {

    public WebClient webClient;

    public CreateHashController(WebClient webClient) {
        this.webClient = webClient;
    }

    @GetMapping("/createFile")
    public void createHashFile() throws IOException {
        startProcess();
    }

    private void startProcess() throws IOException {
        String edoFile = "src/main/resources/file/business.csv";
        Path hashFilePath = Paths.get("src/main/resources/file/business_hashFile.cvs");
        Long init = System.currentTimeMillis();
        List<String> hashList = getHashList(Files.readAllLines(Paths.get(edoFile), StandardCharsets.UTF_8));
        System.out.println("elements => " + hashList.size());
        System.out.println("End process => " + ( (System.currentTimeMillis() - init) / 1000));
        Files.write(hashFilePath,hashList, Charset.defaultCharset());
    }

    private List<String> getHashList(List<String> edoList) {
        Collection<String> edoListSync = Collections.synchronizedCollection(edoList);
        return edoListSync.parallelStream().map(edo -> this.webClient.get().uri(edo).accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(Hash.class)
                .map(Hash::getHashedAgreementId)
                .block()).collect(Collectors.toList());
    }
}
