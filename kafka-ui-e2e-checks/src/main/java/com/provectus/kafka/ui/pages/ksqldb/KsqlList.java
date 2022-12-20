package com.provectus.kafka.ui.pages.ksqldb;

import static com.codeborne.selenide.Selenide.$x;

import com.codeborne.selenide.Condition;
import com.codeborne.selenide.SelenideElement;
import com.provectus.kafka.ui.pages.BasePage;
import io.qameta.allure.Step;
import java.util.Arrays;

public class KsqlList extends BasePage {

  protected SelenideElement tablesTab = $x("//nav[@role='navigation']/a[contains(text(),'Tables')]");
  protected SelenideElement streamsTab = $x("//nav[@role='navigation']/a[contains(text(),'Streams')]");
  protected SelenideElement executeKsqlRequestButton = $x("//button[text()='Execute KSQL Request']");


  @Step
  public KsqlList waitUntilScreenReady(){
    Arrays.asList(tablesTab,streamsTab).forEach(element -> element.shouldBe(Condition.visible));
    return this;
  }

  @Step
  public KsqlList openQueryPage(){
    executeKsqlRequestButton.shouldBe(Condition.enabled).click();
    return this;
  }
}
