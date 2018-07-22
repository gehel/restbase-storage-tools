package org.wikimedia.restbase.krv;

import java.io.File;
import java.io.IOException;

import org.ini4j.Ini;

import com.google.common.base.Optional;

class Cqlshrc {
    private final String username;
    private final String password;
    private final String cert;

    private Cqlshrc(Ini ini) {
        this(ini.get("authentication", "username"), ini.get("authentication", "password"), ini.get("ssl", "certfile"));
    }

    Cqlshrc(String username, String password, String cert) {
        this.username = username;
        this.password = password;
        this.cert = cert;
    }

    Optional<String> getUsername() {
        return Optional.of(username);
    }

    Optional<String> getPassword() {
        return Optional.of(password);
    }

    Optional<String> getClientCert() {
        return Optional.of(cert);
    }

    static Cqlshrc parse(String path) throws IOException {
        Ini ini = new Ini(new File(path));
        return new Cqlshrc(ini);
    }
}
