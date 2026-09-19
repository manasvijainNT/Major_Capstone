package com.amex.lumi.beam_ingestion.model;
import java.io.Serializable;
import java.util.Objects;

public class Address implements Serializable {

    private String street;
    private String city;
    private String state;
    private String postalCode;
    private String country;

    public Address() {
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Address)) {
            return false;
        }

        Address address = (Address) o;

        return Objects.equals(street, address.street)
                && Objects.equals(city, address.city)
                && Objects.equals(state, address.state)
                && Objects.equals(postalCode, address.postalCode)
                && Objects.equals(country, address.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                street,
                city,
                state,
                postalCode,
                country
        );
    }

}


