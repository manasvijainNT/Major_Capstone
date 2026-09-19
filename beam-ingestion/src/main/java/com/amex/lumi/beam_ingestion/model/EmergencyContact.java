package com.amex.lumi.beam_ingestion.model;
import java.io.Serializable;
import java.util.Objects;

public class EmergencyContact implements Serializable {

    private String name;
    private String relationship;
    private String phone;
    private String email;

    public EmergencyContact() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRelationship() {
        return relationship;
    }

    public void setRelationship(String relationship) {
        this.relationship = relationship;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof EmergencyContact)) {
            return false;
        }

        EmergencyContact contact = (EmergencyContact) o;

        return Objects.equals(name, contact.name)
                && Objects.equals(relationship, contact.relationship)
                && Objects.equals(phone, contact.phone)
                && Objects.equals(email, contact.email);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                name,
                relationship,
                phone,
                email
        );
    }
}


