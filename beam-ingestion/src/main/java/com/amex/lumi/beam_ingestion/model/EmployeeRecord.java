package com.amex.lumi.beam_ingestion.model;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class EmployeeRecord implements Serializable {

    private String employeeId;
    private String firstName;
    private String lastName;
    private String email;
    private String phoneNumber;
    private String hireDate;
    private String department;
    private String jobTitle;
    private String salary;
    private String currency;
    private String employmentStatus;
    private String managerId;
    private Boolean isActive;

    private List<String> skills;

    private Address address;

    private EmergencyContact emergencyContact;

    private String ingestionTimestamp;
    private String executionId;
    private String sourceCreationTime;

    public EmployeeRecord() {
    }

    public String getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(String employeeId) {
        this.employeeId = employeeId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getHireDate() {
        return hireDate;
    }

    public void setHireDate(String hireDate) {
        this.hireDate = hireDate;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public String getJobTitle() {
        return jobTitle;
    }

    public void setJobTitle(String jobTitle) {
        this.jobTitle = jobTitle;
    }

    public String getSalary() {
        return salary;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getEmploymentStatus() {
        return employmentStatus;
    }

    public void setEmploymentStatus(String employmentStatus) {
        this.employmentStatus = employmentStatus;
    }

    public String getManagerId() {
        return managerId;
    }

    public void setManagerId(String managerId) {
        this.managerId = managerId;
    }

    public Boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(Boolean active) {
        isActive = active;
    }

    public List<String> getSkills() {
        return skills;
    }

    public void setSkills(List<String> skills) {
        this.skills = skills;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public EmergencyContact getEmergencyContact() {
        return emergencyContact;
    }

    public void setEmergencyContact(EmergencyContact emergencyContact) {
        this.emergencyContact = emergencyContact;
    }

    public String getIngestionTimestamp() {
        return ingestionTimestamp;
    }

    public void setIngestionTimestamp(String ingestionTimestamp) {
        this.ingestionTimestamp = ingestionTimestamp;
    }

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public String getSourceCreationTime() {
        return sourceCreationTime;
    }

    public void setSourceCreationTime(String sourceCreationTime) {
        this.sourceCreationTime = sourceCreationTime;
    }

    @Override
    public String toString() {
        return "Record{" +
                "employeeId='" + employeeId + '\'' +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", email='" + email + '\'' +
                ", phoneNumber='" + phoneNumber + '\'' +
                ", hireDate='" + hireDate + '\'' +
                ", department='" + department + '\'' +
                ", jobTitle='" + jobTitle + '\'' +
                ", salary=" + salary +
                ", currency='" + currency + '\'' +
                ", employmentStatus='" + employmentStatus + '\'' +
                ", managerId='" + managerId + '\'' +
                ", isActive=" + isActive +
                ", skills=" + skills +
                ", ingestionTimestamp='" + ingestionTimestamp + '\'' +
                ", executionId='" + executionId + '\'' +
                ", sourceCreationTime='" + sourceCreationTime + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof EmployeeRecord)) {
            return false;
        }

        EmployeeRecord employee = (EmployeeRecord) o;

        return Objects.equals(employeeId, employee.employeeId)
                && Objects.equals(firstName, employee.firstName)
                && Objects.equals(lastName, employee.lastName)
                && Objects.equals(email, employee.email)
                && Objects.equals(phoneNumber, employee.phoneNumber)
                && Objects.equals(hireDate, employee.hireDate)
                && Objects.equals(department, employee.department)
                && Objects.equals(jobTitle, employee.jobTitle)
                && Objects.equals(salary, employee.salary)
                && Objects.equals(currency, employee.currency)
                && Objects.equals(employmentStatus, employee.employmentStatus)
                && Objects.equals(managerId, employee.managerId)
                && Objects.equals(isActive, employee.isActive)
                && Objects.equals(skills, employee.skills)
                && Objects.equals(address, employee.address)
                && Objects.equals(emergencyContact, employee.emergencyContact)
                && Objects.equals(ingestionTimestamp, employee.ingestionTimestamp)
                && Objects.equals(executionId, employee.executionId)
                && Objects.equals(sourceCreationTime, employee.sourceCreationTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                employeeId,
                firstName,
                lastName,
                email,
                phoneNumber,
                hireDate,
                department,
                jobTitle,
                salary,
                currency,
                employmentStatus,
                managerId,
                isActive,
                skills,
                address,
                emergencyContact,
                ingestionTimestamp,
                executionId,
                sourceCreationTime
        );
    }
}





